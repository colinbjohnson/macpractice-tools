# Overview
The MacPractice Attachment Export Tool is a script that will copy all MacPractice attachments into a given directory - and name these attachments in a human-readable format.

In version MacPractice 5.0.17 all attachments are saved within a disk image using an attachment hash, similar to the following "`00a0dd39860c703bb2bc70ec63ebb8399a987e25e3e2cc1b8dc106bbee626c32`." When uploaded, a filename might have original been "`Colin - Insurance Card Scan - 2015-05-19`" but is stored on disk "`00a0dd39860c703bb2bc70ec63ebb8399a987e25e3e2cc1b8dc106bbee626c32`." The attachment hash does not provide any information about the attachment itself. This creates problems when attempting to work with attachments outside of MacPractice, or export all MacPractice attachments to move to a new EMR/EHR system.

# How It Works
The MacPractice Attachment Export Tool works by:

1. examining the filename of each attachment
2. locating the attachment hash in the MacPractice database
3. collecting information about the given attachment
4. copying the attachment to a new directory
5. naming the copied attachment with a human readable name - currently lastname-firstname-person_id-original_filename

# Use of MacPractice Attachment Export Tool 
An example use is below:

`./macpractice_attachment_export.py --server 172.16.201.132 --username mp_user --password my_password --source-dir /path/to/mp_attachments --target-dir /path/to/export_folder`

# Limitations:
Currently, MacPractice Attachment Export tool has been tested with only:

1. Image Attachments
2. Order Attachments

# Disclaimer:
Use this tool at your own risk.

# Feature Requests and Bug Reports:
Please feel free file Feature Requests and Bug Reports if they are encountered.
