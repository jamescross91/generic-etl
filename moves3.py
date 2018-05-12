import boto3

s3 = boto3.resource('s3')
SOURCE_BUCKET = "fendix-data-warehouse"
DEST_BUCKET = "fendix-mongo-bucket"

bucket = s3.Bucket(SOURCE_BUCKET)
objects = bucket.objects.all()

for obj in objects:
    if obj.key == '201702.csv' or obj.key == '201703.csv' or obj.key == '201704.csv':
        print("Moving " + obj.key)
        source_path = SOURCE_BUCKET + "/" + obj.key
        s3.Object(DEST_BUCKET, obj.key).copy_from(CopySource=source_path)
        s3.Object(SOURCE_BUCKET, obj.key).delete()