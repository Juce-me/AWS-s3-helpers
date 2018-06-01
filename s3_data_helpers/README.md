AWS S3 Data Helpers
=======

Requrements
---
boto3

Description
---
S3 helpers to do some work with Data Transfer Files v.2 (DCM Logs) files stored at S3.

boto3_s3_download_to_local.py
=======

Requrements
-----
>sys, threading, os
>boto3

Description
-----
Takes files from s3://bucket/folder/\*.specific_extension and downlods it to local folder.

Hot to use
-----
Open, use your creds in the client dictionary.


-----