import sys
import threading
import os
import boto3

client = {
  'client1': {
    'name': 'client1',
    'outputBucket': 'test-aws-bucket-name', #aws s3 bucket
    'bucketFolder': 'bucket_folder/', #aws s3 buket folder
    'fileExt': '.gz', #aws s3 file extension to download
    'localFolder': '/Clients/client1/data/dataFiles/' #local path
  }
}


s3 = boto3.resource('s3')
class ProgressPercentage(object):
  def __init__(self, filename):
    self._filename = filename
    self._seen_so_far = 0
    self._lock = threading.Lock()
  def __call__(self, bytes_amount):
    # To simplify we'll assume this is hooked up
    # to a single filename.
    with self._lock:
      self._seen_so_far += bytes_amount
      sys.stdout.write(
        "\r%s --> %s bytes transferred" % (
          self._filename, self._seen_so_far))
      sys.stdout.flush()

def gets3logsList(client_cfg):
  s3objects = []
  outputBucket = client_cfg['outputBucket']
  folderBucket = client_cfg['bucketFolder']
  #======
  s3 = boto3.resource('s3')
  s3bucket = s3.Bucket(outputBucket)
  for file in s3bucket.objects.all():
    currentFolder = file.key.split('/')[0]
    if currentFolder == folderBucket.split('/')[0]: 
      s3objects.append(file.key.split('/')[1])
  return s3objects

def getLocalLogsList(client_cfg):
  downloadedLogs = []
  for root, dirs, files in os.walk(client_cfg['localFolder']):
    for file in files:
      if file.endswith(client_cfg['fileExt']):
        downloadedLogs.append(file)
  return downloadedLogs

def downloadFilesFromS3(client_cfg, date = '20180312'):
  s3 = boto3.resource('s3')
  downloadBucket = client_cfg['outputBucket']
  inputFolder = client_cfg['localFolder']
  clientFolder = client_cfg['bucketFolder'].split('/')[0]
  s3bucket = s3.Bucket(downloadBucket)
  downloadedLogs = getLocalLogsList(client_cfg)
  get_blobs = []
  for file in s3bucket.objects.all():
    currentFolder = file.key.split('/')[0]
    if currentFolder == clientFolder and file.key.endswith(client_cfg['fileExt']): 
      blobName = file.key.split('/')[1]
      blobDate = blobName.split('_')[-4][:8]
      blobType = blobName.split('_')[2]
      ## more than date
      if int(blobDate) >= int(date) and blobName not in downloadedLogs:
        download_file = inputFolder + blobName
        try:
          boto3.client('s3').download_file(downloadBucket, file.key, download_file, Callback = ProgressPercentage(download_file))
        except:
          sys.stdout.write('Ooopps. File Object does not excist.')
      else:
        sys.stdout.write(
          "\r {} --> Skipping. File already in place.".format(blobName))
        sys.stdout.flush()
  status = 'downloaded'
  return status

client_cfg = GM['client1']
print downloadFilesFromS3(client_cfg)