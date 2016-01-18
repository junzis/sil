#!/bin/bash
yesterday=$(date +%Y_%m_%d -d "yesterday")
year=$(date +%Y -d "yesterday")
month=$(date +%m -d "yesterday")

db="SIL_${yesterday}"

folder=/var/www/dumps/${year}/${year}_${month}/
mkdir -p ${folder}
mongodump --db "${db}" --collection "${yesterday}" --out - | gzip > ${folder}/${yesterday}_raw.gz

# drop database 7 days ago
olddate=$(date +%Y_%m_%d -d "yesterday -7 days")
olddb="SIL_${olddate}"
mongo ${olddb} --eval "db.dropDatabase()"

# delete data from 2 months ago, if existing
oldyear=$(date +%Y -d "-2 month")
oldmonth=$(date +%m -d "-2 month")
oldfolder=/var/www/dumps/${oldyear}/${oldyear}_${oldmonth}
rm -rf ${oldfolder}