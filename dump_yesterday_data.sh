#!/bin/bash
yesterday=$(date +%Y%m%d -d "yesterday")
year=$(date +%Y -d "yesterday")
month=$(date +%m -d "yesterday")

datafolder=/home/adsb/workspace/sil/data

folder=/mnt/500G/www/dumps/${year}/${year}_${month}/
mkdir -p ${folder}

folder2=/mnt/3T/sil_dumps/${year}/${year}_${month}/
mkdir -p ${folder2}

gzip ${datafolder}/ADSB_RAW_${yesterday}.csv
gzip ${datafolder}/EHS_RAW_${yesterday}.csv

cp ${datafolder}/ADSB_RAW_${yesterday}.csv.gz ${folder}
cp ${datafolder}/EHS_RAW_${yesterday}.csv.gz ${folder}

cp ${datafolder}/ADSB_RAW_${yesterday}.csv.gz ${folder2}
cp ${datafolder}/EHS_RAW_${yesterday}.csv.gz ${folder2}

# remove data from one week ago
olddate=$(date +%Y%m%d -d "yesterday -7 days")
rm ${datafolder}/ADSB_RAW_${olddate}.csv.gz
rm ${datafolder}/EHS_RAW_${olddate}.csv.gz

# delete data from 4 months ago, if existing
oldyear=$(date +%Y -d "-4 month")
oldmonth=$(date +%m -d "-4 month")
oldfolder=/mnt/500G/www/dumps/${oldyear}/${oldyear}_${oldmonth}
rm -rf ${oldfolder}