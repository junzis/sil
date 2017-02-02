"""
decode an ADS-B dump using multiprocessing:
python decode_adsb_multi_process.py [input_raw_adsb_dump.csv] [output_file_name.csv]

Decrease CHUNKSIZE for low memory computer.
"""

from __future__ import print_function
import sys
import pandas as pd
import numpy as np
import pyModeS as pms
import multiprocessing
import warnings
warnings.filterwarnings("ignore")

COLS = ['ts', 'icao', 'callsign', 'lat', 'lon', 'alt', 'spd', 'hdg', 'roc']

CHUNKSIZE = 1000000
N_PARTITIONS = 10

try:
    fin = sys.argv[1]
    fout = sys.argv[2]
except:
    print("usage: python decode-adsb.py [input_file] [output_file]")
    sys.exit()


def getv(msg):
    """return velocity params from message"""
    v = pms.adsb.velocity(msg)
    return pd.Series({'spd': v[0], 'hdg': v[1], 'roc': v[2]})


def process_chunk(df_raw):
    pname = multiprocessing.Process().name

    print("%s: %d number of messages" % (pname, df_raw.shape[0]))

    # select TypeCode  9 - 18, position messages
    df_pos_raw = df_raw[df_raw['tc'].between(9, 18)]
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
                p = pms.adsb.position(last_even_msg, last_odd_msg, last_even_time, last_odd_time)
                if not p:
                    continue

                if last_even_time > last_odd_time:
                    ts = last_even_time
                    alt = pms.adsb.altitude(last_even_msg)
                else:
                    ts = last_odd_time
                    alt = pms.adsb.altitude(last_odd_msg)

                postitions.append({
                    'ts': round(ts, 1),
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

    # fliter by TypeCode 19, velocity messages
    df_spd_raw = df_raw[df_raw['tc']==19]
    df_spd_raw.loc[:, 'ts_rounded'] = df_spd_raw['ts'].round().astype(int)
    df_spd_raw.drop_duplicates(['ts_rounded', 'icao'], inplace=True)
    df_spd_raw.drop('ts', axis=1, inplace=True)

    # merge velocity message to decoded positions
    df_merged = df_pos_decoded.merge(df_spd_raw, on=['ts_rounded', 'icao'])
    df_merged.drop(df_merged['msg'].isnull(), axis=0, inplace=True)
    df_merged = df_merged.join(df_merged['msg'].apply(getv))

    print("%s: decoding callsigns..." % pname)

    # decode callsign
    df_callsign_raw = df_raw[df_raw['tc'].between(1, 4)]
    df_callsign_raw.loc[:, 'ts_rounded'] = df_callsign_raw['ts'].round().astype(int)
    df_callsign_raw.drop_duplicates(['ts', 'icao'], inplace=True)
    df_callsign_raw['callsign'] = df_callsign_raw['msg'].apply(pms.adsb.callsign)
    df_callsign = df_callsign_raw.drop(['ts', 'msg'], axis=1)

    df_merged = df_merged.merge(df_callsign, on=['ts_rounded', 'icao'])

    df_merged = df_merged[COLS]

    return df_merged

def parallelize_df(df, func, n_partitions):
    df_split = np.array_split(df, n_partitions)
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


df_adsb_chunks = pd.read_csv(fin, names=['ts','icao', 'tc', 'msg'], chunksize=CHUNKSIZE)

with open(fout, 'w') as f:
    f.write(','.join(COLS) + '\n')

for df_adsb in df_adsb_chunks:
    df_out = parallelize_df(df_adsb, process_chunk, N_PARTITIONS)
    df_out.sort_values(['ts', 'icao'], inplace=True)

    print("Append to csv file: %s, %d lines\n" % (fout, df_out.shape[0]))
    df_out.to_csv(fout, mode='a', index=False, header=False)
