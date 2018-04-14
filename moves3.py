import boto3

s3 = boto3.resource('s3')
SOURCE_BUCKET = "fendix-data-warehouse"
DEST_BUCKET = "fendix-mongo-bucket"

bucket = s3.Bucket(SOURCE_BUCKET)
objects = bucket.objects.all()

for obj in objects:
    print("Moving " + obj.key)
    source_path = SOURCE_BUCKET + "/" + obj.key
    s3.Object(DEST_BUCKET, obj.key).copy_from(CopySource=source_path)
    s3.Object(SOURCE_BUCKET, obj.key).delete()