from platform import version
import boto3
import json
import argparse
import requests


def init_client():
  client = boto3.client(
      "s3",
      aws_access_key_id="ASIAYS2NSKMNZUT6M3VJ",
      aws_secret_access_key="4cYgnyq82VUWu2/qVdvt90f6rlYj4zGf52ghXgfz",
      aws_session_token=
      '''IQoJb3JpZ2luX2VjEBMaCXVzLXdlc3QtMiJHMEUCIDchnRABwAUkzNYjYZsctnQ2pDHfWS3RIhZI/o/9B7ZxAiEAhrDhFUsMMQEskWrF6blLDKv/0CgeiUNZ8OUPB2FKnuUqqgIIXBAAGgw1OTAxODM4MTM5MTUiDGs72Xhbu8bPjR2FBCqHApzuuiJax+hYzkRSzfYTO3AOGU/QZRCc2pV+6R5ERfHeHxmmOWK0cgOC1jhB9HGGkMwfzbz3N3aM9NnLcA1NzWz/bSdZpTF22sQts/7Fgy/LsUXiQxYJUaFQnUVWQi3PSQ+gjQIJFzRvw9BjyN7CRzg1AOeA06+iky1HwPR8z4iVbyZim+LFoFF1glAYduEN7i6jKKqQqWzxADx2atEFPjl+DUIs7Wxlr3e3RtjHA4Gje2EKVhVf/vgKXZNLlzma3uvaNHLgpB2b6O8aMJc5CI5BxoZwRaeWtBA3TgYcuQnyKFuj2qAW2rD7hbaqF57sOH96woerZ70mx9lkYcZirHnhGIgv/zubMNKImbEGOp0BcXBRYl/k3aL1wgR5e3T93ztNfdvMAej2hWkFXc4XXCBE2d8D2bpqaSx7vB8/AHdvEZ1MJi6aoxU0WQe+TV8cRm05o28rAEa7zHsGKcwcwHz/9ViLNZG79l1ERUwZdmWc/7TFPxwJka6hPCn1iWLthwrmEOvvqtVLe3BOkWPcKtfEbAKbqzaPxmZwEPcgB+C/ryT9Cysu0hoXxo31+w==''',
      region_name="us-east-1")
  return client


#examples:
#python main.py -f bucket_exists -b fdfgfdgfdgdgdfgd
#python main.py -f count_extensions_usage  -b fdfgfdgfdgdgdfgd
#python main.py -f upload_file_from_url -b fdfgfdgfdgdgdfgd  -k your_object_key.png --file_url https://letsenhance.io/static/8f5e523ee6b2479e26ecc91b9c25261e/1015f/MainAfter.jpg


#work
def bucket_exists(bucket_name):
  s3_client = init_client()
  try:
    s3_client.head_bucket(Bucket=bucket_name)
    print(f"{bucket_name} bucket exist")
    return True
  except Exception as e:
    print(e)
    return False


#work
def delete_bucket(bucket_name):
  s3_client = init_client()
  try:
    s3_client.delete_bucket(Bucket=bucket_name)
    return True
  except Exception as e:
    print(e)
    return False


#work
def create_bucket(bucket_name):
  s3_client = init_client()
  try:
    s3_client.create_bucket(Bucket=bucket_name)
    return True
  except Exception as e:
    print(e)
    return False


#work
def delete_object(bucket_name, object_key):
  s3_client = init_client()
  try:
    s3_client.delete_object(Bucket=bucket_name, Key=object_key)
    return True
  except Exception as e:
    print(e)
    return False


#work
def enable_versioning(bucket_name):
  s3_client = init_client()
  try:
    s3_client.put_bucket_versioning(
        Bucket=bucket_name, VersioningConfiguration={'Status': 'Enabled'})
    return True
  except Exception as e:
    print(e)
    return False


#work
def disable_versioning(bucket_name):
  s3_client = init_client()
  try:
    s3_client.put_bucket_versioning(
        Bucket=bucket_name, VersioningConfiguration={'Status': 'Suspended'})
    return True
  except Exception as e:
    print(e)
    return False


#work
def promote_version(bucket_name, object_key, version_id):
  s3_client = init_client()
  try:
    s3_client.copy_object(Bucket=bucket_name,
                          CopySource={
                              'Bucket': bucket_name,
                              'Key': object_key,
                              'VersionId': version_id
                          },
                          Key=object_key,
                          MetadataDirective='REPLACE')
    return True
  except Exception as e:
    print(e)
    return False


#work
def list_object_versions(bucket_name, object_key):
  s3_client = init_client()
  try:
    response = s3_client.list_object_versions(Bucket=bucket_name,
                                              Prefix=object_key)
    objects = response.get('Versions', [])
    versions = [{
        'Key': obj['Key'],
        'VersionId': obj['VersionId']
    } for obj in objects]
    print(versions)
    return versions
  except Exception as e:
    print(e)
    return None


#work
def grant_bucket_permissions(bucket_name, permission):
  s3_client = init_client()
  try:
    if permission == 'READ_PUBLIC':
      policy = {
          "Version":
          "2012-10-17",
          "Statement": [{
              "Sid": "PublicReadGetObject",
              "Effect": "Allow",
              "Principal": "*",
              "Action": "s3:GetObject",
              "Resource": f"arn:aws:s3:::{bucket_name}/*",
          }],
      }
      s3_client.delete_public_access_block(Bucket=bucket_name)
      s3_client.put_bucket_policy(Bucket=bucket_name,
                                  Policy=json.dumps(policy))
      print(f"READ permission granted for bucket {bucket_name}")
      return True
    elif permission == 'PRIVATE':
      policy = {
          "Version":
          "2012-10-17",
          "Statement": [{
              "Effect": "Deny",
              "Principal": "*",
              "Action": "s3:GetObject",
              "Resource": f"arn:aws:s3:::{bucket_name}/*",
              "Condition": {
                  "Bool": {
                      "aws:SecureTransport": "false"
                  }
              }
          }]
      }
      s3_client.put_bucket_policy(Bucket=bucket_name,
                                  Policy=json.dumps(policy))
      print(f"Private read permission granted for bucket {bucket_name}")
      return True
    elif permission == 'WRITE':
      print("WRITE permission is not supported by this function.")
      return False
    else:
      print("Invalid permission specified.")
      return False
  except Exception as e:
    print("Error granting bucket permissions:", e)
    return False


#work
def upload_fileobj_from_path(bucket_name, object_key, file_path):
  s3_client = init_client()
  try:
    with open(file_path, 'rb') as f:
      s3_client.upload_fileobj(f, bucket_name, object_key)
    return True
  except Exception as e:
    print(e)
    return False


#work
def list_objects(bucket_name):
  s3_client = init_client()
  try:
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    for obj in response.get('Contents', []):
      print(obj['Key'])
    return response
  except Exception as e:
    print(e)
    return None


#work
def put_object(bucket_name, object_key, data):
  s3_client = init_client()
  try:
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=data)
    return True
  except Exception as e:
    print(e)
    return False


#work
def download_file(bucket_name, object_key, local_file_path):
  s3_client = init_client()
  try:
    s3_client.download_file(bucket_name, object_key, local_file_path)
    return True
  except Exception as e:
    print(e)
    return False


#work
def count_extensions_usage(bucket_name):
  response = list_objects(bucket_name)
  if response is None:
    return None

  extensions_usage = {}
  for obj in response.get('Contents', []):
    key = obj['Key']
    extension = key.split('.')[-1]
    if extension not in extensions_usage:
      extensions_usage[extension] = {'count': 0, 'usage': 0}
    extensions_usage[extension]['count'] += 1
    extensions_usage[extension]['usage'] += obj['Size']

  result = []
  for extension, usage in extensions_usage.items():
    result.append(
        f"{extension}: {usage['count']}, usage: {usage['usage']} bytes")
  for res in result:
    print(res)


def upload_file_from_url(bucket_name, object_key, file_url):
  s3 = init_client()

  try:
    # Download file from URL
    response = requests.get(file_url, stream=True)
    if response.status_code == 200:
      # Upload file to S3 bucket
      s3.put_object(Bucket=bucket_name, Key=object_key, Body=response.content)
      print("File uploaded successfully to S3 bucket.")
      return True
    else:
      print("Failed to download file from URL:", file_url)
      return False
  except Exception as e:
    print("Error:", e)
    return False


def set_object_access_policy(bucket_name, object_key):
  s3_client = init_client()
  try:
    response = s3_client.put_object_acl(ACL="public-read",
                                        Bucket=bucket_name,
                                        Key=object_key)
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
      print(
          f"ACL set to public-read for object {object_key} in bucket {bucket_name}"
      )
      return True
    return False
  except Exception as e:
    print("Error setting object ACL:", e)
    return False


file_url = 'https://letsenhance.io/static/8f5e523ee6b2479e26ecc91b9c25261e/1015f/MainAfter.jpg'
bucket_name = 'fdfgfdgfdgdgdfgd'
#object_key = 'ansible.cfg'
version_id = 'htdAJhBSMSzHzeu2VcgxhCgOOIbDOKW2'
permission = 'READ_PUBLIC'  #'PRIVATE'
file_path = 'newfile.txt'
object_key = 'file.png'
data = 'new data'
local_file_path = 'new.txt'

#bucket_exists(bucket_name)
#delete_bucket(bucket_name)
#create_bucket(bucket_name)
#delete_object(bucket_name, object_key)
# if enable_versioning(bucket_name):
#   print('versioning is on')
# if disable_versioning(bucket_name):
#   print("versioning is disabled")
# if promote_version(bucket_name, object_key, version_id):
#   print(f'version is chaged on {version_id}')
#list_object_versions(bucket_name, object_key)
# if grant_bucket_permissions(bucket_name, permission):
#   print("bucket permission is granted")
#list_objects(bucket_name)
#upload_fileobj_from_path(bucket_name, object_key, file_path)
#put_object(bucket_name, object_key, data)
#download_file(bucket_name, object_key, local_file_path)
#count_extensions_usage(bucket_name)
#upload_file_from_url(bucket_name, object_key, file_url)

parser = argparse.ArgumentParser(description="AWS S3 Utility")

# Define command-line arguments
parser.add_argument("-f", "--function", help="Function name", required=True)
parser.add_argument("-b", "--bucket_name", help="Bucket name")
parser.add_argument("-k", "--object_key", help="Object key")
parser.add_argument("-v", "--version_id", help="Version ID")
parser.add_argument("-p", "--permission", help="Permission")
parser.add_argument("-fp", "--file_path", help="File path")
parser.add_argument("-d", "--data", help="Data")
parser.add_argument("-lf", "--local_file_path", help="Local file path")
parser.add_argument("-fu", "--file_url", help="File URL")

args = parser.parse_args()

# Execute the function based on the provided arguments
if args.function == "bucket_exists":
  bucket_exists(args.bucket_name)
elif args.function == "delete_bucket":
  delete_bucket(args.bucket_name)
elif args.function == "create_bucket":
  create_bucket(args.bucket_name)
elif args.function == "delete_object":
  delete_object(args.bucket_name, args.object_key)
elif args.function == "enable_versioning":
  enable_versioning(args.bucket_name)
elif args.function == "disable_versioning":
  disable_versioning(args.bucket_name)
elif args.function == "promote_version":
  promote_version(args.bucket_name, args.object_key, args.version_id)
elif args.function == "list_object_versions":
  list_object_versions(args.bucket_name, args.object_key)
elif args.function == "grant_bucket_permissions":
  grant_bucket_permissions(args.bucket_name, args.permission)
elif args.function == "list_objects":
  list_objects(args.bucket_name)
elif args.function == "upload_fileobj_from_path":
  upload_fileobj_from_path(args.bucket_name, args.object_key, args.file_path)
elif args.function == "put_object":
  put_object(args.bucket_name, args.object_key, args.data)
elif args.function == "download_file":
  download_file(args.bucket_name, args.object_key, args.local_file_path)
elif args.function == "count_extensions_usage":
  count_extensions_usage(args.bucket_name)
elif args.function == "set_object_access_policy":
  set_object_access_policy(args.bucket_name, args.object_key)
elif args.function == "upload_file_from_url":
  upload_file_from_url(args.bucket_name, args.object_key, args.file_url)
else:
  print("Invalid function name")
