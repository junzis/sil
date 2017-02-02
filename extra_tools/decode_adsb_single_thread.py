"""
Use this script to decode an ADS-B dump in a single thread:
python decode_adsb_single_thread.py [input_raw_adsb_dump.csv] [output_file_name.csv]
"""

from __future__ import print_function
import sys
import pandas as pd
import pyModeS as pms
import warnings
warnings.filterwarnings("ignore")

COLS = ['ts', 'icao', 'lat', 'lon', 'alt', 'spd', 'hdg', 'roc', 'callsign']


def getv(msg):
    """return velocity params from message"""
    if isinstance(msg, str):
        v = pms.adsb.velocity(msg)
        spd, hdg, roc, _ = v
    else:
        spd, hdg, roc = None, None, None

    return pd.Series({'spd': spd, 'hdg': hdg, 'roc': roc})


try:
    fin = sys.argv[1]
    fout = sys.argv[2]
except:
    print("usage: python decode-adsb.py [input_file] [output_file]")
    sys.exit()

adsbchunks = pd.read_csv(fin,
                         names=['ts','icao', 'tc', 'msg'],
                         chunksize=1000000)
df_out = pd.DataFrame()

for i, df_raw in enumerate(adsbchunks):
    print("\n--------Chunk %d--------" % (i+1))
    print("%d number of messages" % df_raw.shape[0])


    # select TypeCode  9 - 18, position messages
    df_pos_raw = df_raw[df_raw['tc'].between(9, 18)]
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

    print("decoding velocities...")

    # fliter by TypeCode 19, velocity messages
    df_spd_raw = df_raw[df_raw['tc']==19]
    df_spd_raw.loc[:, 'ts_rounded'] = df_spd_raw['ts'].round().astype(int)
    df_spd_raw.drop_duplicates(['ts_rounded', 'icao'], inplace=True)
    df_spd_raw.drop('ts', axis=1, inplace=True)

    # merge velocity message to decoded positions
    df_merged = df_pos_decoded.merge(df_spd_raw, on=['ts_rounded', 'icao'], how='left')
    df_merged = df_merged.join(df_merged['msg'].apply(getv))

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
