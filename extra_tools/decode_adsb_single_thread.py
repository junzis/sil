"""
Decode ADS-B from the SIL RAW dump, run following for arguments
python decode_adsb_single_thread.py -h
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

COLS = ["ts", "icao", "lat", "lon", "alt", "gs", "trk", "roc", "callsign"]

parser = argparse.ArgumentParser()
parser.add_argument("--fin", help="input raw file", required=True)
parser.add_argument("--fout", help="output file path", required=True)
parser.add_argument(
    "--nproc", help="number of proceses", default=multiprocessing.cpu_count()
)
parser.add_argument("--lat0", help="latitude of the receiver", required=True)
parser.add_argument("--lon0", help="longitude of the receiver", required=True)
args = parser.parse_args()

fin = args.fin
fout = args.fout
nproc = int(args.nproc)
lat0 = float(args.lat0)
lon0 = float(args.lon0)

print("-" * 80)
print("receiver position: [%.3f, %.3f]" % (lat0, lon0))
print("input raw file: %s" % fin)
print("output file: %s" % fout)
print("-" * 80)


def get_v(msg):
    """return velocity params from message"""
    try:
        v = pms.adsb.velocity(msg)
        gs, trk, roc, _ = v
    except:
        gs, trk, roc = None, None, None

    return pd.Series({"gs": gs, "trk": trk, "roc": roc})


def decode_raw_file(fraw):
    df_raw = pd.read_csv(fraw, names=["ts", "df", "icao", "msg"])
    df_raw = df_raw.query("df==17 | df==18")
    df_raw["tc"] = df_raw.msg.apply(pms.typecode)

    print("%d number of messages" % df_raw.shape[0])

    # typecode 9-18 airborn position | typecode 5-8 surface position
    df_pos_raw = df_raw[
        (df_raw["tc"].between(9, 18)) | (df_raw["tc"].between(5, 8))
    ].copy()
    df_pos_raw.dropna(inplace=True)
    df_pos_raw.drop_duplicates(["ts", "icao"], keep="last", inplace=True)

    icaos = df_pos_raw["icao"].unique()

    print("number of icao: %d" % len(icaos))

    # identify the ODD / EVEN flag for each message
    df_pos_raw.loc[:, "oe"] = df_pos_raw["msg"].apply(pms.adsb.oe_flag)

    print("decoding positions...")

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
                if pms.adsb.typecode(last_even_msg) != pms.adsb.typecode(
                    last_odd_msg
                ):
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

    print("decoding velocities...")

    # typecode 19 (airborn velocity) | typecode 5-8 (surface velocity)
    df_spd_raw = df_raw[
        (df_raw["tc"] == 19) | (df_raw["tc"].between(5, 8))
    ].copy()
    df_spd_raw.dropna(inplace=True)
    df_spd_raw.loc[:, "ts"] = df_spd_raw["ts"].round(2)
    df_spd_raw.drop_duplicates(["ts", "icao"], keep="last", inplace=True)
    df_spd_raw.loc[:, "ts_rounded"] = df_spd_raw["ts"].round().astype(int)
    df_spd_raw.drop("ts", axis=1, inplace=True)

    # merge velocity message to decoded positions
    merge_type = "left"
    df_spd_raw.drop_duplicates(
        ["ts_rounded", "icao"], keep="last", inplace=True
    )

    df_merged = df_pos_decoded.merge(
        df_spd_raw, on=["ts_rounded", "icao"], how=merge_type
    )

    df_merged = df_merged.join(df_merged["msg"].apply(get_v))

    print("decoding callsigns...")

    # decode callsign
    df_callsign_raw = df_raw[df_raw["tc"].between(1, 4)].copy()
    df_callsign_raw.loc[:, "ts_rounded"] = (
        df_callsign_raw["ts"].round().astype(int)
    )
    df_callsign_raw.drop_duplicates(["ts", "icao"], keep="last", inplace=True)
    df_callsign_raw["callsign"] = df_callsign_raw["msg"].apply(
        pms.adsb.callsign
    )
    df_callsign = df_callsign_raw.drop(["ts", "msg"], axis=1).copy()

    df_merged = df_merged.merge(
        df_callsign, on=["ts_rounded", "icao"], how="left"
    )

    df_merged.drop_duplicates(
        ["icao", "lat", "lon", "gs", "trk", "roc"], keep="last", inplace=True
    )

    df_merged = df_merged[COLS]

    df_merged = df_merged.sort_values("ts")

    return df_merged


if __name__ == "__main__":

    df_out = decode_raw_file(fin)
    print("Writing to csv file: %s, %d lines\n" % (fout, df_out.shape[0]))
    df_out.to_csv(fout, index=False)
