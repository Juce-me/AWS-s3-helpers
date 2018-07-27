#processinglogs.py
# -*- coding: utf-8 -*-
#requrements: 
import boto
import boto3
from datetime import datetime, timedelta
import time
from functools import wraps
import os
import csv
import cStringIO

# timings and logs
def timing(f):
  @wraps(f)
  def wrap(*args):
    time1 = time.time()
    ret = f(*args)
    time2 = time.time()
    runtime = (time2-time1)*1000.0
    functionName = f.func_name
    status =  '%s function took %0.3f ms' % (functionName, runtime)
    print '\n\t' + status
    return ret, runtime, functionName
  return wrap

#functions
def logsOpen(brand, prefix, logsDir):
  #=====
  #set client name
  startTime = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
  logName = brand + '_' + prefix + '_' + startTime + '.csv'
  if not os.path.exists(logsDir):
    os.makedirs(logsDir)
  logPath = os.path.join(logsDir, logName)
  logFile = open(logPath, 'wb')
  print 'Start execution at {}. \nWriting timing logs to {}'.format(startTime, logPath)
  #write as csv file
  logWriter = csv.writer(logFile)
  logWriter.writerow(['date','time','#','brand','Function name','Status','Run time','fileType','checked','processed'])
  return logFile, logWriter, logName, logPath

def logsWrite(logWriter, num, client, functionName, status, runTime, fileType='', checked='', processed=''):
  Now = datetime.today()
  dateNow = Now.strftime('%Y-%m-%d')
  timeNow = Now.strftime('%H:%M:%S')
  logWriter.writerow([dateNow, timeNow, num, client, functionName, status, runTime, fileType, checked, processed])

def transferLogsToBucket(logName, logPath, client_cfg, local_file = 'file'):
  #=====
  # modification for boto3
  s3 = boto3.resource('s3')
  bucket_name_output = client_cfg['outputBucket']
  folderBucket = client_cfg['logsFolder']
  #=====
  # write file to object contents
  src_uri = boto.storage_uri(os.path.join(logPath), local_file)
  object_contents = cStringIO.StringIO()
  src_uri.get_key().get_file(object_contents)
  object_contents.seek(0,0)
  #=====
  # transfer to AWS
  s3.Bucket(bucket_name_output).put_object(Key = folderBucket + logName, Body = object_contents)
  #=====
  object_contents.close()
  status =  'Transfer log file {} was transfered to {}'.format(logName, folderBucket + logName)
  return status

def logsClose(logFile):
  logFile.close()