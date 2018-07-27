AWS Helpers
=======

Description
---
Simple helpers to make routing work faster for Data Scientist/Miner/whatever (if needed). In case you need to do some operations fast and there is no time to think.

## S3 data helpers


### S3 download to local

Just transfer those S3 files to local storage.

**Requirements**
- boto3

-----

## DCM DTFv2.0 data transfer

Basically it was made to filter DCM files by advertiser ID and store it in the GS. 
But current version is made to FILTER and transfer DTFv2.0 files to Amazon S3 storage.

How it works
----

- Filters: it takes DCM advertiser IDs and Floodlight IDs and checks if line of the DTF file contains it, or not.
- Processing: it handles all downloaded files in memory (no additional storage needed on your server).
- Logging: it writes down 2 types of logs
  - Processing logs with timing of processing different DTF types.
  - Error logs with date-time when error occurred. For current version only general errors processed and logged.
  - Logs could be stored in the S3 folder bucket as well.

What to do
----

- fill in clients variable for clients you need to process.
- install gsutil, boto3, write down your GS, S3 credentials to the _.boto_.
- run main script.

**Requirements**

* boto
* boto3
* os
* gzip
* cStringIO
* tempfile
* csv
* time
* functools
* datetime

-----