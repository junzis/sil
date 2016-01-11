#!/bin/bash
yesterday=$(date +%Y_%m_%d -d "yesterday")
year=$(date +%Y -d "yesterday")
month=$(date +%m -d "yesterday")

db="SIL_${yesterday}"

folder=/var/www/dumps/${year}/${year}_${month}/
mkdir -p ${folder}
mongodump --db "${db}" --collection "${yesterday}" --out - | gzip > ${folder}/${yesterday}_raw.gz

# drop yesterday's database
mongo ${db} --eval "db.dropDatabase()"

# delete data from 2 months ago, if existing
oldyear=$(date +%Y -d "-2 month")
oldmonth=$(date +%m -d "-2 month")
oldfolder=/var/www/dumps/${oldyear}/${oldyear}_${oldmonth}
rm -rf ${oldfolder}