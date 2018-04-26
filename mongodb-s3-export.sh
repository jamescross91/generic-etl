#!/usr/bin/env bash

DBNAME=segmentation

BUCKET=fendix-mongo-bucket

COLLECTION=`/bin/date -d yesterday +%Y%m`

TEMP_DIR=/home/ec2-user

# Log
echo "Backing up $DBNAME/$COLLECTION to s3://$BUCKET/";

/usr/bin/mongoexport --db segmentation --collection $COLLECTION --type=csv --fields "_id",browser,datetime,groups.0,groups.1,groups.2,groups.3,groups.4,groups.5,ip,referring_url,trust,userhash,version --out $TEMP_DIR/$COLLECTION.csv
/usr/bin/aws s3 cp $TEMP_DIR/$COLLECTION.csv s3://$BUCKET
rm $TEMP_DIR/$COLLECTION.csv