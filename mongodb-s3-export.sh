#!/usr/bin/env bash

DBNAME=segmentation

BUCKET=fendix-mongo-bucket

COLLECTION=`/bin/date -d yesterday +%Y%m`

TEMP_DIR=/home/ec2-user

# Log
echo "Backing up $DBNAME/$COLLECTION to s3://$BUCKET/";

/usr/bin/mongoexport --db segmentation --collection $COLLECTION --type=csv --fields "_id",browser,datetime,groups.0,groups.1,groups.2,groups.3,groups.4,groups.5,groups.6,groups.7,groups.8,groups.9,groups.10,groups.11,groups.12,groups.13,groups.14,groups.15,groups.16,groups.17,groups.18,groups.19,groups.20,ip,referring_url,trust,userhash,version --out $TEMP_DIR/$COLLECTION.csv
/usr/bin/aws s3 cp $TEMP_DIR/$COLLECTION.csv s3://$BUCKET
rm $TEMP_DIR/$COLLECTION.csv