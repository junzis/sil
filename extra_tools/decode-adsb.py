"""
Use this script to decode an ADS-B dump:
python decode-adsb.py [input_raw_adsb_dump.csv] [output_file_name.csv]
"""

from __future__ import print_function
import sys
import pandas as pd
import pyModeS as pms
import warnings
warnings.filterwarnings("ignore")

def getv(msg):
    """return velocity params from message"""
    v = pms.adsb.velocity(msg)
    return pd.Series({'spd': v[0], 'hdg': v[1], 'roc': v[2]})

try:
    fin = sys.argv[1]
    fout = sys.argv[2]
except:
    print("usage: python decode-adsb.py [input_file] [output_file]")
    sys.exit()

adsbchunks = pd.read_csv(fin,
                         names=['ts','icao', 'tc', 'adsb_msg'],
                         chunksize=1000000)
dfout = pd.DataFrame()

for i, adsb in enumerate(adsbchunks):
    print("\n--------Chunk %d--------" % (i+1))
    print("%d number of messages" % adsb.shape[0])


    # select TypeCode  9 - 18, position messages
    adsbpos = adsb[adsb['tc'].between(9, 18)]
    adsbpos.drop_duplicates(['ts', 'icao'], inplace=True)

    icaos = adsbpos['icao'].unique()

    print("number of icao: %d" % len(icaos))

    print('Gathering position messages...')

    # indentify the ODD / EVEN flag for each message
    adsbpos.loc[:, 'oe'] = adsbpos['adsb_msg'].apply(pms.adsb.oe_flag)

    print('Decoding positions...')
    postitions = []
    # loop through all aircarft decode there positions
    for i, icao in enumerate(icaos):
        if i % 100 == 0:
            print("%d of %d" % (i, len(icaos)))

        data = adsbpos[adsbpos['icao']==icao]

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
                    'ts': int(ts),
                    'icao': icao,
                    'lat': p[0],
                    'lon': p[1],
                    'alt': alt
                })
            else:
                continue

    adsbpos_decoded = pd.DataFrame(postitions)

    # fliter by TypeCode 19, velocity messages
    adsbspd = adsb[adsb['tc']==19]
    adsbspd.loc[:, 'ts'] = adsbspd['ts'].astype(int)
    adsbspd.drop_duplicates(['ts', 'icao'], inplace=True)

    # merge velocity message to decoded positions
    adsbmerged = adsbpos_decoded.merge(adsbspd, on=['ts', 'icao'])
    adsbmerged.drop(adsbmerged['adsb_msg'].isnull(), axis=0, inplace=True)

    print("Decoding velocities...")
    adsbmerged = adsbmerged.join(adsbmerged['adsb_msg'].apply(getv))
    adsbmerged.drop(['tc', 'adsb_msg'], axis=1, inplace=True)

    dfout = dfout.append(adsbmerged, ignore_index=True)

print("saving csv file: %s" % fout)
cols = ['ts', 'icao', 'lat', 'lon', 'alt', 'spd', 'hdg', 'roc']
dfout = dfout[cols]     # rearrange column orders
dfout.to_csv(fout, index=False)
