"""
Decode an ADS-B dump using multiprocessing, run following for arguments
python decode_adsb_multi_process.py -h

Decrease CHUNKSIZE for low memory computer.
"""


import sys
import pandas as pd
import numpy as np
import pyModeS as pms
import multiprocessing
import argparse
import warnings
warnings.filterwarnings("ignore")

sil_lat = 51.990
sil_lon = 4.375

COLS = ['ts', 'icao', 'lat', 'lon', 'alt', 'spd', 'hdg', 'roc', 'callsign']

CHUNKSIZE = 1000000
N_PARTITIONS = 10

parser = argparse.ArgumentParser()
parser.add_argument('--fin', help="input csv file", required=True)
parser.add_argument('--fout', help="output csv file", required=True)
parser.add_argument('--mergeon', help="merge on postion or velocity",
                    default='pos', choices=['pos', 'v'])
parser.add_argument('--lat0', help="latitude of the receiver base", default=sil_lat)
parser.add_argument('--lon0', help="longitude of the receiver base", default=sil_lon)
args = parser.parse_args()

fin = args.fin
fout = args.fout
mergeon = args.mergeon
lat0 = float(args.lat0)
lon0 = float(args.lon0)

print('receiver position: %.3f, %.3f' % (lat0, lon0))

def get_v(msg):
    """return velocity params from message"""
    if isinstance(msg, str):
        v = pms.adsb.velocity(msg)
        spd, hdg, roc, _ = v
    else:
        spd, hdg, roc = None, None, None

    return pd.Series({'spd': spd, 'hdg': hdg, 'roc': roc})

def process_chunk(df_raw):
    pname = multiprocessing.Process().name

    print("%s: %d number of messages" % (pname, df_raw.shape[0]))

    # typecode 9-18 airborn position | typecode 5-8 surface position
    df_pos_raw = df_raw[(df_raw['tc'].between(9, 18)) | (df_raw['tc'].between(5, 8))]
    df_pos_raw.dropna(inplace=True)
    df_pos_raw.drop_duplicates(['ts', 'icao'], inplace=True)

    icaos = df_pos_raw['icao'].unique()

    print("%s: number of icao: %d" % (pname, len(icaos)))

    # identify the ODD / EVEN flag for each message
    df_pos_raw.loc[:, 'oe'] = df_pos_raw['msg'].apply(pms.adsb.oe_flag)

    print('%s: decoding positions...' % pname)

    postitions = []
    # loop through all aircarft decode there positions
    for icao in icaos:
        data = df_pos_raw[df_pos_raw['icao']==icao]

        last_even_msg = ''
        last_odd_msg = ''
        last_even_time = 0
        last_odd_time = 0

        for d in data.values:
            if d[4] == 0:
                last_even_msg = d[3]
                last_even_time = d[0]
            else:
                last_odd_msg = d[3]
                last_odd_time = d[0]

            if abs(last_even_time - last_odd_time) < 10:
                if pms.adsb.typecode(last_even_msg) != pms.adsb.typecode(last_odd_msg):
                    continue

                p = pms.adsb.position(last_even_msg, last_odd_msg,
                                      last_even_time, last_odd_time,
                                      lat0, lon0)

                if not p:
                    continue

                if last_even_time > last_odd_time:
                    ts = last_even_time
                    alt = pms.adsb.altitude(last_even_msg)
                else:
                    ts = last_odd_time
                    alt = pms.adsb.altitude(last_odd_msg)

                postitions.append({
                    'ts': round(ts, 2),
                    'ts_rounded': int(round(ts)),
                    'icao': icao,
                    'lat': p[0],
                    'lon': p[1],
                    'alt': alt
                })
            else:
                continue

    df_pos_decoded = pd.DataFrame(postitions)

    print("%s: decoding velocities..." % pname)

    # typecode 19 (airborn velocity) | typecode 5-8 (surface velocity)
    df_spd_raw = df_raw[(df_raw['tc']==19) | (df_raw['tc'].between(5, 8))]
    df_spd_raw.dropna(inplace=True)
    df_spd_raw.loc[:, 'ts'] = df_spd_raw['ts'].round(2)
    df_spd_raw.drop_duplicates(['ts', 'icao'], inplace=True)
    df_spd_raw.loc[:, 'ts_rounded'] = df_spd_raw['ts'].round().astype(int)

    # merge velocity message to decoded positions
    if mergeon == 'pos':
        merge_type = 'left'
        df_spd_raw.drop('ts', axis=1, inplace=True)
        df_spd_raw.drop_duplicates(['ts_rounded', 'icao'], inplace=True)
    elif mergeon == 'v':
        merge_type = 'right'
        df_pos_decoded.drop('ts', axis=1, inplace=True)
        df_pos_decoded.drop_duplicates(['ts_rounded', 'icao'], inplace=True)

    df_merged = df_pos_decoded.merge(df_spd_raw, on=['ts_rounded', 'icao'], how=merge_type)
    df_merged = df_merged.join(df_merged['msg'].apply(get_v))

    print("%s: decoding callsigns..." % pname)

    # decode callsign
    df_callsign_raw = df_raw[df_raw['tc'].between(1, 4)]
    df_callsign_raw.loc[:, 'ts_rounded'] = df_callsign_raw['ts'].round().astype(int)
    df_callsign_raw.drop_duplicates(['ts', 'icao'], inplace=True)
    df_callsign_raw['callsign'] = df_callsign_raw['msg'].apply(pms.adsb.callsign)
    df_callsign = df_callsign_raw.drop(['ts', 'msg'], axis=1)

    df_merged = df_merged.merge(df_callsign, on=['ts_rounded', 'icao'], how='left')

    df_merged.drop_duplicates(['icao', 'lat', 'lon', 'spd', 'hdg', 'roc'], inplace=True)

    df_merged = df_merged[COLS]

    return df_merged

def parallelize_df(df, func, n_partitions):
    df_split = np.array_split(df, n_partitions)
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


if __name__ == '__main__':
    df_adsb_chunks = pd.read_csv(fin, names=['ts','icao', 'tc', 'msg'], chunksize=CHUNKSIZE)

    with open(fout, 'w') as f:
        f.write(','.join(COLS) + '\n')

    for df_adsb in df_adsb_chunks:
        df_out = parallelize_df(df_adsb, process_chunk, N_PARTITIONS)
        df_out.sort_values(['ts', 'icao'], inplace=True)

        print("Append to csv file: %s, %d lines\n" % (fout, df_out.shape[0]))
        df_out.to_csv(fout, mode='a', index=False, header=False)
