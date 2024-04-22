import boto3
import argparse

def init_client():
  client = boto3.client(
    "s3",
    aws_access_key_id="ASIA3FLD2VFJZ7A4SJP4",
    aws_secret_access_key="jZlu2YmyNwUMjGNmNofgtlBI024N6UooWhPDDFlN",
    aws_session_token="IQoJb3JpZ2luX2VjEOT//////////wEaCXVzLXdlc3QtMiJFMEMCHzwYY39p0bIXqmOaTPaUnzhHKyuvz/ZY4P89aka1SwYCIBxXtUB1HWVZoJCOi1Z1iS3HgzHrVGVwhS6o/LH5J2OHKq8CCC0QABoMNzY3Mzk3ODk4NTc5IgwGXjYwW8/wjLUIFuIqjAK5LrNHPcsqJ3YDEMqCNwUgs4f7ZdtNDjJ4tF8OevyV7DxIHJpt7u54uHde7Q62AwENA/HyUAYOB0/XM/aIB5AwCElXFOCvZBpWkmvOI1eEQRxfw9ju2K10T/NHGDg+wWRLZXN8ycGN0IQQh8l5GPTZ58fZkTkoz9zXgAmiG6CZjdhJg7m+skyi/ePXXgZ6pTgVW1WSQEWHwURhQBFxxJKImjYezQ7tPo/kYRQ86YA3P7XLoq/hWJ2zJ8ycQvlAmiWwwLzMgs8Jxtq4Uhx5GD10juB05dTZflAOD/FNHmUZNyZWjLI42eOQK2BtFZFjLVUTNgeCgIih8ulCFaPCXRSvSYhg2qHtxT684e3GMIjbjrEGOp8BMG8drvALY6o9X2woVXqx517Zq0mX5NSzR5WVUQNWLTX52SWc5IohXHaQ6JOy+6Cp25/T0Re7EtSPfowEGvJuE0l3H1yc6TKBDAjZ2YcTBjQEUpGrq0hwhJzV8+38x1H4eLZCyZ0qi0Yokq3hRmVjMuON5Rw9AfEhmb9RwheTx+blAONDSWS94lYCyvGlVIthimo0l/+z10nn9YACfNbJ",
    region_name="us-east-1"
  )
  client.list_buckets()

  return client

def bucket_exists(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except Exception as e:
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check if a bucket exists and delete it if it does.")
    parser.add_argument("bucket_name", type=str, help="Name of the bucket to check/delete")
    args = parser.parse_args()


    if bucket_exists(args.bucket_name):
        print(f"Bucket exists.")
    else:
        print(f"Bucket does not exist.")

