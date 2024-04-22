import boto3
import configparser
import argparse
import requests

# Configparser რო წაიკითხოს სეკრეტებიდან AWS-ის გასაღებები
config = configparser.ConfigParser()
config.read('secrets.ini')

AWS_ACCESS_KEY_ID = config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']
# AWS_SESSION_TOKEN = config['AWS']['AWS_SESSION_TOKEN'] - Session token ლაბიდან თუ ვმუშაობთ მაშინ დაგვჭირდება და კლიენტის ინიციალიზირების დროს შეგვიძლია მივუთითოთ
AWS_REGION_NAME = config['AWS']['AWS_REGION_NAME']

#ფუნქცია S3 კლიენტის ინიციალიზირებისთვის, ქმნის S3 კლიენტს და აბრუნებს მას
def get_s3_client():
  client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    #aws_session_token=AWS_SESSION_TOKEN, - აქ იქნებოდა სესიის ტოკენი
    region_name=AWS_REGION_NAME
  )

  return client

# ფუნქცია რომელიც ამოწმებს არსებობს თუ არა S3 bucket, თუ არსებობს ფუნქცია წარმატებით დასრულდება და დააბრუნებს True-ს, თუ არ არსებობს Exception-ს ამოაგდებს და დააბრუნებს False-ს
def does_bucket_exist(s3, bucket_name):
  try:
    s3.head_bucket(Bucket=bucket_name)
    return True
  except:
    return False
  
# ფუნქცია სათითაო ფაილის წასაშლელად S3 bucket-დან
def delete_file_in_bucket(s3, bucket_name, file_name):
  try:
    s3.delete_object(Bucket=bucket_name, Key=file_name)
  except Exception as e:
    return e
  
# ფუნქცია რომელიც Bucket-ში არსებულ ფაილებს შეხედავს და შემდეგ მათი სახელების ლისტს/მასივს დააბრუნებს
def list_files_in_bucket(s3, bucket_name):
  try:
    response = s3.list_objects(Bucket=bucket_name)

    # response-ში არის ლისტი/მასივი სახელად Contents, ამ Contents გადაუვლის for ლუპით, თითოეული ელემენტი არის დიქშინარი, რომელსაც აქვს Key ველი და ეს ველი წარმოადგენს ფაილის სახელს
    file_names = [object['Key'] for object in response['Contents']]
    return file_names
  except Exception as e:
    return e
  
# ფუნქცია რომელიც გაასუფთავებს და წაშლის გადაცემულ S3 Bucket-ს
def delete_bucket(s3, bucket_name):
  try:
    # იმისათვის რო Bucket-ის წაშლა შევძლოთ, პირველ რიგში ის ცარიელი უნდა იყოს, აქ უკვე დაწერილი list_files_in_bucket ფუნქციით ვამოწმებთ Bucket-ში არსებული ფაილების სიას
    bucket_files = list_files_in_bucket(s3, bucket_name)

    # bucket_files არის ლისტი/მასივი იმ ფაილების სახელების, რომლებიც არსებობენ ამ Bucket-ში, აქ კი ამ ფაილებს სათითაოდ გადავუვლით და წავშლით Bucket-დან
    if bucket_files:
      for file in bucket_files:
        delete_file_in_bucket(s3, bucket_name, file)

    # მას შემდეგ რაც გავასუფთავეთ Bucket, შეგვიძლია ახლა თავად Bucket-ც წავშალოთ
    s3.delete_bucket(Bucket=bucket_name)
    return "Bucket Deleted"
  except Exception as e:
    print(e)
    return "Bucket Does Not Exist"
  
# ფუნქცია ვერსიონირების ჩართვისათვის S3 Bucket-ში
def turn_versioning_on_off(s3, bucket_name, state):
  # state მომხმარებლის მიერ გადაცემული სტრინგია, რომელიც ან on უნდა იყოს ან off, რის შედეგადაც ვაკვლევთ ვერსიონირების სტატუს Enabled უნდა იყოს თუ Suspended. Enabled არის ექვივალენტი ჩართულისა, ხოლო Suspended არის ექვივალენტი გამორთულისა, on და off სტრინგებს უბრალოდ გამარტივებისთვის ვიღებთ
  if state == 'on':
    status = 'Enabled'
  elif state == 'off':
    status = 'Suspended'
  else:
    # თუ გადაცემული სტრინგი არ არის on ან off, მაშინ აღარ შეეხება Bucket-ს და უბრალოდ დააბრუნებს Invalid State-ს
    return "Invalid State"

  # მას შემდეგ რაც დავადგინეთ, რომ გადაცემული სტრინგი ვალიდურია და შევქმენით status ცვლადი, ვუცვლით ვერსიონირების სტატუსს Bucket-ს და ვაბრუნებთ Done სტრინგს
  try:
    s3.put_bucket_versioning(
      Bucket=bucket_name,
      VersioningConfiguration={
        'Status': status
      }
    )
    return "Done"
  except Exception as e:
    return e
  
# ფუნქცია რომელიც შეამოწმებს Bucket-ში არსებული ფაილების ვერსიებს
def list_file_versions(s3, bucket_name, file_name):
  try:
    # პირველ რიგში პასუხად ველოდებით დიქშინარის/ობიექტს რომელშიც იქნება Versions ველი
    response = s3.list_object_versions(Bucket=bucket_name, Prefix=file_name)
    # ცალკე ცვლადად ვიღებთ ამ Versions ველს
    versions = response['Versions']
    # for ლუპით გადავუვლით ამ versions ველს, რომელშიც არის დიქშინარების ლისთი/ერეი, თითოეულ ელემენტს აქვს VersionId და ამ VersionId-ს ვინახავთ version_ids ლისტში და შემდეგ ვაბრუნებთ ამ ლისტს/ერეის
    version_ids = [v['VersionId'] for v in versions]
    return version_ids
  except Exception as e:
    return e

# ფუნქცია რომელიც ცვლის ვერსიის ფაილს ID-თ
def change_file_version_with_id(s3, bucket_name, file_name, version_id):
  # თუ ვიცით ფაილის ვერსიის აიდი, რომლითაც გვინდა რომ Bucket-ში არსებული ამჟამინდელი ფაილი ჩავანაცვლოთ, მაშინ Bucket-დან ვაკოპირებთ ფაილის ამ ვერსიას ID-თ და ვანაცვლებთ ამჟამინდელ ვერსიას
  try:
    s3.copy_object(
      CopySource={
        'Bucket': bucket_name,
        'Key': file_name
      },
      Bucket=bucket_name,
      Key=file_name,
      VersionId=version_id
    )
    return "Done"
  except Exception as e:
    return e
  
# ფუნქცია რომელიც ატვირთავს ფაილს S3 ბაქეთში
def upload_file_object(s3, bucket_name, file_name):
  # upload_fileobj-ს შეუძლია ფაილის ატვირთვა როგორც url-დან, ასევე memory-ში არსებული ფაილებიდან და შეუძლია ფაილის ნაწილ-ნაწილ ატვირთვა
  try:
    s3.upload_fileobj(
      Fileobj=open(file_name, 'rb'),
      Bucket=bucket_name,
      Key=file_name
    )

    file_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"

    return file_url
  except Exception as e:
    return e
  
# ფუნქცია რომელიც ლოკალურ ფაილს ატვირთავს S3 ბაქეთში
def put_file_object(s3, bucket_name, file_name):
  # upload_file ერთიანად ტვირთავს ფაილს S3 Bucket-ში, ფაილი ზოგადად არის ლოკალური
  try:
    s3.upload_file(
      Filename=file_name,
      Bucket=bucket_name,
      Key=file_name
    )

    file_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"

    return file_url
  except Exception as e:
    return e
  
# ფუნქცია რომელიც ლინკიდან გადმოწერს ფაილს და ლოკალურად ჩაიწერს
def download_file_from_url(url):
  try:
    file_name = url.split('/')[-1]

    response = requests.get(url, stream=True)

    with open(file_name, "wb") as f:
        f.write(response.content)

    return file_name

  except Exception as e:
    return e

# ფუნქცია რომელიც ფაილს ჩამოტვირთავს ლოკალურად და შემდეგ ატვირთავს S3 Bucket-ში
def upload_file_from_url(s3, bucket_name, url):
  try:
    # უკვე არსებული ფუნქციით გადმოწერს ფაილს ლინკიდან და დააბრუნებს ადგილმდებარეობას/სახელს
    file_name = download_file_from_url(url)

    # ფაილის გადმოწერის შემდეგ ატვირთავს მას S3 Bucket-ში, ასევე შეგვეძლო გამოგვეყენებინა უკვე დაწერილი ატვირთვის ფუნქციები
    s3.upload_file(
      Filename=file_name,
      Bucket=bucket_name,
      Key=file_name
    )

    file_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"

    return file_url
  except Exception as e:
    return e
  
# ფუნქცია რომელიც დაითვლის თუ რამდენი ფაილი არსებობს კონკრეტული გაფართოებებით S3 Bucket-ში და ასევე მათ ზომებს
def count_extension_usage_in_bucket(s3, bucket_name):
  try:
    # წამოიღებს ფაილებს Bucket-ში და ამოიღებს Contents ველით ამ ლისთს, ასევე შეგვეძლო გამოგვეყენებინა უკვე დაწერილი ფუქნცია რომელიც სახელებს აბრუნებს
    files = s3.list_objects(Bucket=bucket_name)['Contents']

    # წინასწარ ინიციალიზირებული ცარიელი ობიექტი/დიქშინარი
    extensions = {}

    # გადაუვლის ფაილებს for ლუპით
    for file in files:
      # Key არის ფაილის სახელი, ზოგადად ფაილში ბოლო წერტილის შემდეგ არსებული სტრინგი არის ამ ფაილის გაფართოება, აქ ჩვენ ფაილის სახელს ვყოფთ ლისტად ყველა ადგილში სადაც წერტილი გვხვდება, მაგალითად file.txt გაყოფის შემდეგ იქნება ['file', 'txt'] და ამ ლისტიდან ვიღებთ ბოლო ელემენტს [-1] ინდექსით, რომელიც არის გაფათოება
      extension = file['Key'].split('.')[-1]
      # აქვე ვიღებთ ფაილის ზომას, რომელიც არის ბაიტებში და გადაგვყავს მეგაბაიტებში
      size = int(file['Size'])
      size_in_mb = size / 1024 / 1024

      # მას შემდეგ რაც ვიცით, ფაილის გაფართოება და ზომა, ვამოწმებთ წინასწარ ინიციალიზირებულ extensions ობიექტში/დიქშინარიში არის თუ არა უკვე არსებული ველი ამ გაფართოებით. თუ არ არის, მაშინ ვქმნით ახალ ველს, მაგალითად extensions: { "txt": { "count": 1, "size": 1" } }, სადაც count რაოდენობა 1-ია რადგან ახლახანს შეიქმნა, ხოლო size არის პირველი ფაილის ზომა მეგაბაიტებში. თუ გაფართოების ველი არსებობს, მაშინ ამ გაფართოების count-ს გავზრდით ერთს და საერთო ზომას დავუმატებთ ახალ ზომას.
      if extension in extensions:
        extensions[extension]['count'] += 1
        extensions[extension]['size'] = extensions[extension]['size'] + size_in_mb
      else:
        extensions[extension] = { 'count': 1, 'size': size_in_mb }

    return extensions
  except Exception as e:
    return e

if __name__ == '__main__':
  # არგუმენტების პარსერის ინიციალიზირება
  parser = argparse.ArgumentParser()

  # არგუმენტების პარსერს ვამატებს ველს, რომელიც უნდა გადაეცეს, მაგალითად -bn არის bucket_name, fn არის file_name და ა.შ.
  parser.add_argument('-bn')
  # parser.add_argument('-fn')
  # parser.add_argument('-url')
  # parser.add_argument('-vs')


  # ვპარსავთ არგუმენტებს, args იქნება დიქშინარი/ობიექტი გადაცემული არგუმენტებისა
  args = parser.parse_args()

  # ვიღებთ არგუმენტებს გაპარსული args-დან და გადავცმეთ შესაბამის ფუნქციას
  bucket_name = args.bn
  # file_name = args.fn
  # file_url = args.url
  # versioning_state = args.vs

  s3 = get_s3_client()
  # print(does_bucket_exist(s3, bucket_name))
  # print(delete_bucket(s3, bucket_name))
  # print(turn_versioning_on_off(s3, bucket_name, versioning_state))
  # print(list_file_versions(s3, bucket_name, file_name))
  # print(upload_file_object(s3, bucket_name, file_name))
  # print(put_file_object(s3, bucket_name, file_name))
  # print(upload_file_from_url(s3, bucket_name, file_url))
  print(count_extension_usage_in_bucket(s3, bucket_name))