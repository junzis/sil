#!/bin/bash
db='SIL'
coll=$(date +%Y_%m_%d -d "yesterday")
year=$(date +%Y -d "yesterday")
month=$(date +%m -d "yesterday")
folder = /var/www/dumps/${year}/${year}_${month}/
mkdir -p ${folder}
# mongodump --db "${db}" --collection "${coll}" --out - | gzip > "/var/www/dumps/01_Raw_Messages/${coll}.gz"
mongodump --db "${db}" --collection "${coll}" --out - | gzip > "${folder}/${coll}_raw.gz"