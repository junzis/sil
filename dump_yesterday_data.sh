#!/bin/bash
db='SIL'
coll=$(date +%Y-%m-%d -d "yesterday")
mongodump --db "${db}" --collection "${coll}" --out - | gzip > "/var/www/dumps/01_Raw_Messages/${coll}.gz"