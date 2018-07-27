# -*- coding: utf-8 -*-
# python 2.7
import boto
import boto3
import os
import gzip
import cStringIO
import tempfile
import csv

#timing
import time
from functools import wraps

from datetime import datetime, timedelta
import sys


#import logs workers
from logsWorker import logsOpen, logsWrite, transferLogsToBucket, logsClose, timing

def required_dates():
#for downloading job
  d2 = datetime.now() - timedelta(days = 1)
  d1 = d2 - timedelta(days = 1)
  d1 = d1.strftime("%Y%m%d")
  d2 = d2.strftime("%Y%m%d")
  return int(d1),int(d2)

# -----------------------------------------
def getLogsList(bucket_name, GOOGLE_STORAGE = 'gs'):
  # get local stored logs
  uploaded_blobs = []
  try:
    uri = boto.storage_uri(bucket_name, GOOGLE_STORAGE)
    for blob in uri.get_bucket():
      uploaded_blobs.append(blob.name)
  except:
    status = 'Error get logs list'
    print status
  return uploaded_blobs

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
      try:
        s3objects.append(file.key.split('/')[1])
      except:
        print 'Nothing in the s3 folder or folder'
  return s3objects

#inputBucket', 'advIDs', 'outputBucket', 'bucketKeys'
@timing
def downloadFilesByDateCSV(client_cfg, type, logwriter, errwriter, yyyymmdd1 = 20180701, yyyymmdd2 = 20180701, gcs = 'gs'):
  #==== 
  # modification for boto3
  s3 = boto3.resource('s3')
  bucket_name_input = client_cfg['inputBucket']
  bucket_name_output = client_cfg['outputBucket']
  folderBucket = client_cfg['bucketFolder']
  clientName = client_cfg['name']
  adv_ids = client_cfg['advIDs']
  floodlightCfg_ids = client_cfg['floodlightCfgID']
  toBucket = s3.Bucket(bucket_name_output)
  status = 'Starting to work through files'
  #====
  blobs = getLogsList(bucket_name_input)
  # working with s3
  # =====
  # if you need re-download and re-process all files
  # remove comment below and comment next uncommented line
  #outputBlobs = [''] 
  # get not_saved to s3 files:
  outputBlobs = gets3logsList(client_cfg)
  #get all filenames in storage bucket:
  i = 0
  for blob in blobs:
    #get name, parse date and file type 
    blobName = blob
    blobDate = blobName.split('_')[-4][:8]
    blobType = blobName.split('_')[2] #or ['activity', 'click', 'impression', 'match', 'rich']
    csv_file_name = blobName.replace('.gz','')
    #download file if its in the range of from-to date and in type array
    if yyyymmdd1 <= int(blobDate) <= yyyymmdd2 and blobType == type:
      i += 1
      if csv_file_name not in outputBlobs:
        #try:
        #### — change format
        sys.stdout.write("\r -> Processing File: {0}>".format(blobName))
        #####
        #print '\r\t-> Processing file: {}'.format(blobName)
        #get path for the file from the storage
        src_uri = boto.storage_uri(os.path.join(bucket_name_input, blobName), gcs)
        try:
          object_contents = cStringIO.StringIO()
          #place file in memory
          src_uri.get_key().get_file(object_contents)
          #filter source file in memory
          #NOT completely OPTIMIZED FOR MATCH FILES — it's going to gunzip it and gz it again. 
          #config path for new file
          newKey = folderBucket + blobName
          if type != 'match':
            #filtered content container
            objectContentsInput = cStringIO.StringIO()
            [objectContentsInput, checked, processed, status], runTime, functionName = processGZLogsInMemory(object_contents, adv_ids, floodlightCfg_ids, blobName, blobType)
            logsWrite(logwriter, str(i), clientName, functionName, status, runTime, blobType, checked, processed)
            #archive file
            '''config
              fgz — gz container
              objectContentUnput - input csv file
              gzip_obj - gz container
            '''
            fgz = cStringIO.StringIO()
            objectContentsInput.seek(0,0)
            gzip_obj = gzip.GzipFile(filename = newKey, mode = 'wb', fileobj = fgz)
            gzip_obj.write(objectContentsInput.getvalue())
            gzip_obj.close()
            #  ====
            #save file to aws s3
            #upload file object
            fgz.seek(0,0)
            obj = toBucket.Object(newKey)
            obj.upload_fileobj(fgz)
            fgz.close()
            objectContentsInput.close()
          else:
            #skipping unarchiving and archiving file for match files
            object_contents.seek(0,0)
            obj = toBucket.Object(newKey)
            obj.upload_fileobj(object_contents)
            object_contents.close()
          #close source file
          object_contents.close()
          status = 'Transfered file to s3: {} {}'.format(folderBucket, blobName)
          runtime = ''
          functionName = 'upload'
        except Exception as e:
          status = 'Error: {}'.format(e)
          errorDate = datetime.now().strftime("%Y%m%d,%X")
          functionName = 'Download'
          logsWrite(errwriter, str(i), clientName, functionName, status, errorDate, blobType)
      else: 
        status = 'Skipped file {}'.format(blobName)
        runtime = ''
        functionName = 'Skiped file'
        logsWrite(logwriter, str(i), clientName, functionName, status, runtime, blobType)   
    else:
      status = 'No logs in the data or file exists'
      functionName = 'upload'
  return status

#----using boto to work with gs and local files. 

#process gz to csv, check type and filter it by adv_ids
@timing
def processGZLogsInMemory(gz_object_contents_input, adv_ids, floodlight_ids, blob_name, blob_type):
  gz_object_contents_input.seek(0)
  csv_object_contents_output = cStringIO.StringIO()
  #decompress file 
  raw_object_contents_output = gzip.GzipFile(fileobj = gz_object_contents_input, mode = 'rb')
  #read as csv
  logreader = csv.reader(raw_object_contents_output)
  #save headers
  headers = next(logreader)
  try:
    #looking for column with advertiser id
    ADID_col_num = headers.index('Advertiser ID')
  except:
    ADID_col_num = -1
  try:
    floodlightIDs_num = headers.index('Floodlight Configuration')
  except:
    floodlightIDs_num = -1
  #----------------------
  rowsChecked, rowsProcessed = 0, 0
  if ADID_col_num != -1: 
    #filtering rows
    csv_object_contents_output = cStringIO.StringIO()
    filtered_csv = csv.writer(csv_object_contents_output)
    #write headers
    filtered_csv.writerow(headers)
    #filter floodlight id containg files with floodlight ids.
    if floodlightIDs_num > 0:
      for line in logreader:
        rowsChecked += 1
        if line[floodlightIDs_num] in floodlight_ids:
          rowsProcessed += 1
          filtered_csv.writerow(line)
    #filter files contaings advertiser ids 
    else:
      for line in logreader:
        rowsChecked += 1
        if line[ADID_col_num] in adv_ids:
          rowsProcessed += 1
          filtered_csv.writerow(line)
    csv_object_contents_output.seek(0,0)
    pStatus = 'File process: {},Type: {},Rows check: {},Rows saved: {}'.format(blob_name, blob_type, rowsChecked, rowsProcessed)
    status = 'File processed: {} '.format(blob_name)
  else:
    status = 'File was not processed: {},Type: {}'.format(blob_name, blob_type)
    print '\n\t <- ' + status
  #
  try:
    sys.stdout.write("\r <- {0} ".format(status))
  except:
    sys.stdout.write("\r <- {0} ".format(pStatus))
  raw_object_contents_output.close()
  return csv_object_contents_output, rowsChecked, rowsProcessed, status
