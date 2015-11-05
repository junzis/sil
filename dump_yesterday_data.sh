#!/bin/bash
yesterday=$(date +%Y_%m_%d -d "yesterday")
year=$(date +%Y -d "yesterday")
month=$(date +%m -d "yesterday")

db="SIL_${yesterday}"

folder=/var/www/dumps/${year}/${year}_${month}/
mkdir -p ${folder}
mongodump --db "${db}" --collection "${yesterday}" --out - | gzip > ${folder}/${yesterday}_raw.gz