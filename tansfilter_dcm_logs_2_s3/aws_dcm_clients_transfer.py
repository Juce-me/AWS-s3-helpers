# -*- coding: utf-8 -*-
import gcsTransfer2s3
import os

#vars for mainFunction
fileTypes = ['activity','impression','click','rich','match']
fromDate, tillDate = gcsTransfer2s3.required_dates()

# for test 
#fileTypes = ['activity'] #for testing
#fromDate, tillDate = 20180601, 20180801 #for test

#-----------
#Fill this variable. Every iterm is client configuration. 
clients = {
  'clientBRAND': {
    'name': 'client_name_brand',
    'inputBucket': 'dcdt_-dcm_account001', #googles storage bucket for your dcm logs
    'advIDs': ['000001', '000001'], #Advertiser ids
    'floodlightCfgID': ['00000', '00001'], #Floodlight Configuration https://www.google.com/dfa/trafficking/#/accounts/CLIENTS_ACCOUNT/advertisers?status=1
    'outputBucket': 'dcm-logs-bucket', #aws s3 bucket
    'bucketFolder': 'clients_folder/', #create this folder in bucket. MUST exsits
    'logsFolder': 'logs_folder/' #create this folder in bucket. MUST exists
  },
  'clientBRAND2': {
    'name': 'client_name_brand',
    'inputBucket': 'dcdt_-dcm_account002', #g storage
    'advIDs': ['151541', '0002', '000001'], #adv ids
    'floodlightCfgID': ['00001'], #Floodlight Configuration https://www.google.com/dfa/trafficking/#/accounts/CLIENTS_ACCOUNT/advertisers?status=1
    'outputBucket': 'logs-dcm-bucket', #aws s3 bucket
    'bucketFolder': 'clients2_folder/', #another clients folder
    'logsFolder': 'logs/' #aws s3 logs folder
  }
}

for brand in clients:
  # ---------------------------------
  # - creating log files on server
  logsComputeDir = '../logs'
  logfile, logwriter, logname, logpath = gcsTransfer2s3.logsOpen(brand, 'processing', logsComputeDir)
  errLogsComputeDir = '../logs/errors'
  errLogfile, errwriter, errlogname, errLogpath = gcsTransfer2s3.logsOpen(brand, 'error', errLogsComputeDir)
  #for main function number == 0
  j = 0
  clientName = clients[brand]['name']
  for currentType in fileTypes:
    #for test delete fromDate/tillDate
    status, runtime, functionName = gcsTransfer2s3.downloadFilesByDateCSV(clients[brand], currentType, logwriter, errwriter, fromDate, tillDate)
    gcsTransfer2s3.logsWrite(logwriter, str(j), clientName, functionName, status, runtime, currentType)
    
  print "\rFiles ready for {} ".format(brand)
  try:
   status = gcsTransfer2s3.transferLogsToBucket(logname, logpath, clients[brand])
  except Exception as e:
    status =  'Transfer the log file to S3 borken. Error: {} .'.format(e)
  finally:
    print status
    #sending logs
  try:
   status = gcsTransfer2s3.transferLogsToBucket(errlogname, errLogpath, clients[brand])
  except Exception as e:
    status =  'Transfer the error log file to S3 borken. Error: {} .'.format(e)
  finally:
    print status
  gcsTransfer2s3.logsClose(errLogfile)
  gcsTransfer2s3.logsClose(logfile)