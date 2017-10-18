"""
Use this script to decode an ADS-B dump in a single thread:
python decode_adsb_single_thread.py [input_raw_adsb_dump.csv] [output_file_name.csv]
"""

from __future__ import print_function
import sys
import pandas as pd
import pyModeS as pms
import argparse
import warnings
warnings.filterwarnings("ignore")

sil_lat = 51.990
sil_lon = 4.375

COLS = ['ts', 'icao', 'lat', 'lon', 'alt', 'spd', 'hdg', 'roc', 'callsign']

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


def getv(msg):
    """return velocity params from message"""
    if isinstance(msg, str):
        v = pms.adsb.velocity(msg)
        spd, hdg, roc, _ = v
    else:
        spd, hdg, roc = None, None, None

    return pd.Series({'spd': spd, 'hdg': hdg, 'roc': roc})


adsbchunks = pd.read_csv(fin,
                         names=['ts','icao', 'tc', 'msg'],
                         chunksize=1000000)
df_out = pd.DataFrame()

for i, df_raw in enumerate(adsbchunks):
    print("\n--------Chunk %d--------" % (i+1))
    print("%d number of messages" % df_raw.shape[0])


    # typecode 9-18 airborn position | typecode 5-8 surface position
    df_pos_raw = df_raw[(df_raw['tc'].between(9, 18)) | (df_raw['tc'].between(5, 8))]
    df_pos_raw.dropna(inplace=True)
    df_pos_raw.drop_duplicates(['ts', 'icao'], inplace=True)

    icaos = df_pos_raw['icao'].unique()

    print("number of icao: %d" %  len(icaos))

    # identify the ODD / EVEN flag for each message
    df_pos_raw.loc[:, 'oe'] = df_pos_raw['msg'].apply(pms.adsb.oe_flag)

    print('decoding positions...')

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

    print("decoding velocities...")

    # typecode 19 (airborn velocity) | typecode 5-8 (surface velocity)
    df_spd_raw = df_raw[(df_raw['tc']==19) | (df_raw['tc'].between(5, 8))]
    df_spd_raw.dropna(inplace=True)

    df_spd_raw.loc[:, 'ts_rounded'] = df_spd_raw['ts'].round().astype(int)
    df_spd_raw.drop_duplicates(['ts_rounded', 'icao'], inplace=True)
    df_spd_raw.drop('ts', axis=1, inplace=True)

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

    print("decoding callsigns...")

    # fliter by TypeCode 1-4, identification messages
    df_callsign_raw = df_raw[df_raw['tc'].between(1, 4)]
    df_callsign_raw.loc[:, 'ts_rounded'] = df_callsign_raw['ts'].round().astype(int)
    df_callsign_raw.drop_duplicates(['ts', 'icao'], inplace=True)
    df_callsign_raw['callsign'] = df_callsign_raw['msg'].apply(pms.adsb.callsign)
    df_callsign = df_callsign_raw.drop(['ts', 'msg'], axis=1)

    df_merged = df_merged.merge(df_callsign, on=['ts_rounded', 'icao'], how='left')

    df_merged = df_merged[COLS]

    df_out = df_out.append(df_merged, ignore_index=True)

print("saving csv file: %s" % fout)
df_out.sort_values(['ts', 'icao'], inplace=True)
df_out.to_csv(fout, index=False)
