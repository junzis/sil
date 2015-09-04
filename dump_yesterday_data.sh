#!/bin.bash
db='SIL'
coll=$(date +%Y-%m-%d -d "yesterday")
mongodump --db "${db}" --collection "${coll}" --out - | gzip > "/var/www/dumps/${coll}.gz"