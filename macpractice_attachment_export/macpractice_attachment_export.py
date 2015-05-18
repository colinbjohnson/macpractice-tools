#!/usr/bin/env python

import argparse
import logging
import os
import string
import shutil

import mysql.connector

import sys
sys.path.append("../mptools")
from mptools import Attachment, Patient, Person


def export_attachment(attachment, target_dir, file_name, folder_path=None):
    '''
    given an attachment object, a target_dir and a file_name,
    copy the given attachment into that specified target_dir
    '''
    if not os.path.exists("{!s}/{!s}".format(target_dir, folder_path)):
        logging.info('Directory {!s} Will Be Created'.format(folder_path))
        target_path = "{!s}/{!s}".format(target_dir, folder_path)
        os.makedirs(target_path)
    target_file = os.path.join(target_dir, folder_path, file_name)
    logging.info('Will Copy File {!s} to {!s}'.format(attachment.file_path, target_file))
    shutil.copy(attachment.file_path, target_file)
    return True

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='127.0.0.1',
                    help='the IP or DNS Name of the MacPractice MySQL Server.')
parser.add_argument('--username', help='provide a MacPractice DB username.')
parser.add_argument('--password', help='provide a MacPractice DB password.')
parser.add_argument('--database', default='macpractice',
                    help='the name of the MacPractice database.')
parser.add_argument('--dry-run', default=False, dest='dry_run',
                    help='set dry-run True if you wish to to test processing.')
parser.add_argument('--source-dir', default='/Temp/attachments', dest='source_dir',
                    help='the directory where attachments exist.')
parser.add_argument('--target-dir', default='/Temp/attachments_processed', dest='target_dir',
                    help='the directory where attachments should be copied.')
args = parser.parse_args()

# configure logging
log_format = '%(message)s'
logging.basicConfig(level='INFO', format=log_format)


mysql_connection = mysql.connector.connect(user=args.username,
                                           password=args.password,
                                           host=args.server,
                                           database=args.database,
                                           buffered=True)

attachments = {}
# create dictionary of all Patients
patients = Patient.get_all_patients(mysql_connection)
# for each Patient, get last, first
for patient_id in patients:
    person = Person.get_person_by_id(mysql_connection, patients[patient_id].person_id)
    patients[patient_id].last = person.last
    patients[patient_id].first = person.first

# creates a list of all files to be exported from MacPractice
files = os.walk(args.source_dir)
for root, directories, file_names in files:
    for file_name in file_names:
        file_path = os.path.join(root, file_name)
        logging.info('Examining File: {!s}'.format(file_path))
        attachment = Attachment.get_attachment_by_hash(mysql_connection, file_name)
        if attachment is not None:
            attachment_id = attachment.attached_file_id
            attachment.file_path = file_path
            attachment.attachment_type = attachment.get_attachment_type(mysql_connection)
            attachment.patient_id = attachment.get_patient_id(mysql_connection, attachment.attachment_type)
            attachment.image_attachment_type_id = attachment.get_image_attachment_type_id(mysql_connection)
            attachment.image_attachment_type_name = attachment.get_image_attachment_type_name(mysql_connection)
            attachments[attachment_id] = attachment

for attachment_id in attachments:
    attachment = attachments[attachment_id]
    attachment_type = None
    last = None
    first = None
    if attachment.patient_id is not None and patients[attachment.patient_id] is not None:
        last = patients[attachment.patient_id].last
        first = patients[attachment.patient_id].first

    logging.info('Preparing to Export Attachment: {!s}'.format(attachment.attached_file_id))
    if attachment.image_attachment_type_name is not None:
        # slashes ('/') create a problem when copying attachments. Under OS X you can replace with a :
        # I've opted to remove
        attachment_type = string.replace(attachment.image_attachment_type_name, '/', '')
    folder_path = '{!s}-{!s}-{!s}/{!s}'.format(last, first, attachment.patient_id, attachment_type)
    try:
        file_name = '{!s}-{!s}-{!s}'.format(last, first, attachment.file_name)
    except UnicodeEncodeError:
        logging.warn('Failed to encode filename for: {!s}'.format(attachment_id))
        continue
    export_result = export_attachment(attachments[attachment_id], args.target_dir, file_name, folder_path=folder_path)
