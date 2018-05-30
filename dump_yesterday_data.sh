#!/bin/bash
yesterday=$(date +%Y%m%d -d "yesterday")
year=$(date +%Y -d "yesterday")
month=$(date +%m -d "yesterday")
day=$(date +%d -d "yesterday")

datadir=/home/adsb/workspace/sil/data

bakdir1=/mnt/500G/www/dumps/${year}/${year}_${month}/${year}_${month}_${day}
mkdir -p ${bakdir1}

bakdir2=/mnt/3T/sil_dumps/${year}/${year}_${month}/${year}_${month}_${day}
mkdir -p ${bakdir2}

# move to a dedicated folder and compress
mkdir -p ${datadir}/${yesterday}
mv ${datadir}/RAW_${yesterday}_*.csv ${datadir}/${yesterday}
gzip ${datadir}/${yesterday}/*.csv

# copy to backup location
cp ${datadir}/${yesterday}/*.csv.gz ${bakdir1}
cp ${datadir}/${yesterday}/*.csv.gz ${bakdir2}

# remove data from one week ago
olddate=$(date +%Y%m%d -d "yesterday -7 days")
rm -rf ${datadir}/${olddate}

# delete data on Web dir from 4 months ago, if existing
oldyear=$(date +%Y -d "-4 month")
oldmonth=$(date +%m -d "-4 month")
oldfolder=/mnt/500G/www/dumps/${oldyear}/${oldyear}_${oldmonth}
rm -rf ${oldfolder}
