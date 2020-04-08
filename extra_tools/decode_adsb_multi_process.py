"""
Decode an ADS-B dump using multiprocessing, run following for arguments
python decode_adsb_multi_process.py -h
"""

import os
import sys
import glob
import pandas as pd
import numpy as np
import pyModeS as pms
import multiprocessing
import argparse
import warnings

warnings.filterwarnings("ignore")

sil_lat = 51.990
sil_lon = 4.375

COLS = ["ts", "icao", "lat", "lon", "alt", "gs", "trk", "roc", "callsign"]

CHUNKSIZE = 1000000

parser = argparse.ArgumentParser()
parser.add_argument("--rawdir", help="raw data directory", required=True)
parser.add_argument("--outdir", help="output directory", required=True)
parser.add_argument("--year", help="year", required=True)
parser.add_argument("--month", help="month", required=True)
parser.add_argument("--day", help="day", required=True)
parser.add_argument(
    "--nproc", help="number of proceses", default=multiprocessing.cpu_count()
)
parser.add_argument("--lat0", help="latitude of the receiver base", default=sil_lat)
parser.add_argument("--lon0", help="longitude of the receiver base", default=sil_lon)
args = parser.parse_args()

rawdir = args.rawdir
outdir = args.outdir
year = int(args.year)
month = int(args.month)
day = int(args.day)
nproc = int(args.nproc)
lat0 = float(args.lat0)
lon0 = float(args.lon0)

inputdir = "%s/%d/%d_%02d/%d_%02d_%02d/" % (rawdir, year, year, month, year, month, day)
fout = "%s/%d/%d_%02d/ADSB_DECODED_%d%02d%02d.csv" % (
    outdir,
    year,
    year,
    month,
    year,
    month,
    day,
)

os.makedirs("%s/%d/%d_%02d" % (outdir, year, year, month), exist_ok=True)

if os.path.exists(fout):
    print(fout + " already exists. Skipping.")
    sys.exit(1)

print("-" * 80)
print("receiver position: %.3f, %.3f" % (lat0, lon0))
print("input data dir: %s" % inputdir)
print("output data: %s" % fout)
print("-" * 80)


def get_v(msg):
    """return velocity params from message"""
    try:
        v = pms.adsb.velocity(msg)
        gs, trk, roc, _ = v
    except:
        gs, trk, roc = None, None, None

    return pd.Series({"gs": gs, "trk": trk, "roc": roc})


def process_chunk(fraw):
    pname = multiprocessing.Process().name + ": " + fraw[-22:]

    df_raw = pd.read_csv(fraw, names=["ts", "df", "icao", "msg"])
    df_raw = df_raw.query("df==17 | df==18")
    df_raw["tc"] = df_raw.msg.apply(pms.typecode)

    print("%s: %d number of messages" % (pname, df_raw.shape[0]))

    # typecode 9-18 airborn position | typecode 5-8 surface position
    df_pos_raw = df_raw[
        (df_raw["tc"].between(9, 18)) | (df_raw["tc"].between(5, 8))
    ].copy()
    df_pos_raw.dropna(inplace=True)
    df_pos_raw.drop_duplicates(["ts", "icao"], keep="last", inplace=True)

    icaos = df_pos_raw["icao"].unique()

    print("%s: number of icao: %d" % (pname, len(icaos)))

    # identify the ODD / EVEN flag for each message
    df_pos_raw.loc[:, "oe"] = df_pos_raw["msg"].apply(pms.adsb.oe_flag)

    print("%s: decoding positions..." % pname)

    postitions = []
    # loop through all aircarft decode there positions
    for icao in icaos:
        data = df_pos_raw[df_pos_raw["icao"] == icao]

        # print(data.shape)

        last_even_msg = ""
        last_odd_msg = ""
        last_even_time = 0
        last_odd_time = 0

        for d in data.values:
            if d[-1] == 0:
                last_even_msg = d[3]
                last_even_time = d[0]
            else:
                last_odd_msg = d[3]
                last_odd_time = d[0]

            if abs(last_even_time - last_odd_time) < 10:
                if pms.adsb.typecode(last_even_msg) != pms.adsb.typecode(last_odd_msg):
                    continue

                p = pms.adsb.position(
                    last_even_msg,
                    last_odd_msg,
                    last_even_time,
                    last_odd_time,
                    lat0,
                    lon0,
                )

                if not p:
                    continue

                if last_even_time > last_odd_time:
                    ts = last_even_time
                    alt = pms.adsb.altitude(last_even_msg)
                else:
                    ts = last_odd_time
                    alt = pms.adsb.altitude(last_odd_msg)

                postitions.append(
                    {
                        "ts": round(ts, 2),
                        "ts_rounded": int(round(ts)),
                        "icao": icao,
                        "lat": p[0],
                        "lon": p[1],
                        "alt": alt,
                    }
                )
            else:
                continue

    df_pos_decoded = pd.DataFrame(postitions)

    print("%s: decoding velocities..." % pname)

    # typecode 19 (airborn velocity) | typecode 5-8 (surface velocity)
    df_spd_raw = df_raw[(df_raw["tc"] == 19) | (df_raw["tc"].between(5, 8))].copy()
    df_spd_raw.dropna(inplace=True)
    df_spd_raw.loc[:, "ts"] = df_spd_raw["ts"].round(2)
    df_spd_raw.drop_duplicates(["ts", "icao"], keep="last", inplace=True)
    df_spd_raw.loc[:, "ts_rounded"] = df_spd_raw["ts"].round().astype(int)
    df_spd_raw.drop("ts", axis=1, inplace=True)

    # merge velocity message to decoded positions
    merge_type = "left"
    df_spd_raw.drop_duplicates(["ts_rounded", "icao"], keep="last", inplace=True)

    df_merged = df_pos_decoded.merge(
        df_spd_raw, on=["ts_rounded", "icao"], how=merge_type
    )

    df_merged = df_merged.join(df_merged["msg"].apply(get_v))

    print("%s: decoding callsigns..." % pname)

    # decode callsign
    df_callsign_raw = df_raw[df_raw["tc"].between(1, 4)].copy()
    df_callsign_raw.loc[:, "ts_rounded"] = df_callsign_raw["ts"].round().astype(int)
    df_callsign_raw.drop_duplicates(["ts", "icao"], keep="last", inplace=True)
    df_callsign_raw["callsign"] = df_callsign_raw["msg"].apply(pms.adsb.callsign)
    df_callsign = df_callsign_raw.drop(["ts", "msg"], axis=1).copy()

    df_merged = df_merged.merge(df_callsign, on=["ts_rounded", "icao"], how="left")

    df_merged.drop_duplicates(
        ["icao", "lat", "lon", "gs", "trk", "roc"], keep="last", inplace=True
    )

    df_merged = df_merged[COLS]

    return df_merged


def parallelizer(rawfiles, func, nproc):
    pool = multiprocessing.Pool(nproc)
    df = pd.concat(pool.map(func, rawfiles))
    pool.close()
    pool.join()
    return df


if __name__ == "__main__":

    rawfiles = np.array(sorted(glob.glob(inputdir + "*.csv.gz")))
    df_out = parallelizer(rawfiles, process_chunk, nproc)
    df_out.sort_values(["ts", "icao"], inplace=True)

    print("Writing to csv file: %s, %d lines\n" % (fout, df_out.shape[0]))
    df_out.to_csv(fout, index=False)
