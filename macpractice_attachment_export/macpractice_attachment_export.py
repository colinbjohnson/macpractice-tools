#!/usr/bin/env python

import argparse
import logging
import os
import shutil

import mysql.connector


def get_row_result(query):
    '''
    given a MySQL Query, return the result or return None.
    '''
    cursor = mysql_connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    print query
    if result and len(result) == 1:
        return result[0]
    else:
        return None


def get_attachment_type(mysql_connection, attachment_id):
    '''
    given MySQL Connection Object and an Attachment ID, return the type of attachment
    '''
    attachment_type = None
    attachment_type_query = ('SELECT attached_file.attached_file_id, image_attachments.attached_file_id, '
                             'order_attachment_tie.attached_file_id '
                             'FROM attached_file '
                             'LEFT JOIN image_attachments '
                             'ON attached_file.attached_file_id=image_attachments.attached_file_id '
                             'LEFT JOIN order_attachment_tie '
                             'ON attached_file.attached_file_id=order_attachment_tie.attached_file_id '
                             'where attached_file.attached_file_id=\'{!s}\';'.format(attachment_id))
    attachment_type_result = get_row_result(attachment_type_query)
    if attachment_type_result is None:
        logging.warn('attachment_id {!s} is of Type: Unknown.'.format(attachment_id))
    else:
        if attachment_type_result[1] == attachment_id:
            attachment_type = 'image'
            logging.info('attachment_id {!s} is of Type: Image.'.format(attachment_id))
        elif attachment_type_result[2] == attachment_id:
            attachment_type = 'order'
            logging.info('attachment_id {!s} is of Type: Order.'.format(attachment_id))
        else:
            logging.warn('attachment_id {!s} is of Type: Unknown.'.format(attachment_id, attachment_type))
    return attachment_type


def get_image_attachment_row(mysql_connection, attached_file_id):
    '''
    given an attached_file_id return a patient_id
    '''
    image_attachments_row = get_row_result('select attached_file_id, patient_id from image_attachments where '
                                           'attached_file_id=\'{!s}\';'.format(attached_file_id))
    return image_attachments_row


def get_order_patient_row(mysql_connection, attached_file_id):
    '''
    given an attached_file_id return a patient_id
    '''
    order_patient_row = get_row_result('SELECT order_attachment_tie.attached_file_id, '
                                       'order_attachment_tie.orders_id, orders.patient_id '
                                       'FROM order_attachment_tie '
                                       'LEFT JOIN orders '
                                       'ON order_attachment_tie.orders_id=orders.orders_id '
                                       'WHERE order_attachment_tie.orders_id=\'{!s}\';')
    return order_patient_row


def get_attachment_metadata(mysql_connection, attachment_hash):
    '''
    given a mysql_connection and an attachment_hash return attachment data.
    '''
    attachment_metadata = {'attachment_hash': attachment_hash, 'attached_file_id': None, 'file_name': None,
                           'patient_id': None, 'person_id': None,
                           'last_name': None, 'first_name': None, 'attachment_type': None}
    # lookup of attachment_hash
    attached_file_row = get_row_result('select attached_file_id, file_name from attached_file where hash=\'{!s}\''
                                       .format(attachment_hash))
    if attached_file_row is not None:
        attachment_metadata['attached_file_id'] = attached_file_row[0]
        attachment_metadata['file_name'] = attached_file_row[1]
    else:
        logging.warn('attachment_hash {!s} does not exist in attached_file table.'.format(attachment_hash))

    attachment_metadata['attachment_type'] = get_attachment_type(mysql_connection,
                                                                 attachment_metadata['attached_file_id'])

    if attachment_metadata['attached_file_id'] is not None:
        if attachment_metadata['attachment_type'] == 'image':
            image_attachments_row = get_image_attachment_row(mysql_connection, attachment_metadata['attached_file_id'])
            if image_attachments_row is not None:
                attachment_metadata['patient_id'] = image_attachments_row[1]
            else:
                logging.warn('attached_file_id {!s} does not exist in image_attachments table.'
                             .format(attachment_metadata['attached_file_id']))
        elif attachment_metadata['attachment_type'] == 'order':
            order_patient_row = get_order_patient_row(mysql_connection, attachment_metadata['attached_file_id'])
            if order_patient_row is not None:
                attachment_metadata['patient_id'] = order_patient_row[3]
            else:
                logging.warn('attached_file_id {!s} does not exist in orders table.'
                             .format(attachment_metadata['attached_file_id']))
        else:
            logging.warn('attachment_data is not found')

    patient_person_row = get_row_result('SELECT patient.patient_id, patient.person_id, person.last, person.first '
                                        'FROM patient '
                                        'LEFT JOIN person '
                                        'ON patient.person_id=person.person_id '
                                        'WHERE patient.patient_id=\'{!s}\';'
                                        .format(attachment_metadata['patient_id']))
    if patient_person_row is not None:
        attachment_metadata['person_id'] = patient_person_row[1]
        attachment_metadata['last_name'] = patient_person_row[2]
        attachment_metadata['first_name'] = patient_person_row[3]
    else:
        logging.warn('person_id {!s} does not exist in person table.'.format(attachment_metadata['person_id']))
    print attachment_metadata
    return attachment_metadata


def process_attachment(attachment_path, attachment_metadata, target_dir):
    '''
    given: and attachment_path, attachment_metadata and a target_dir,
    creates directory for attachment and copies attachment into directory and uses 'file_name' for file name
    '''
    logging.info('Processing Attachment: {!s}'.format(attachment_path))

    attachment_uid = "{!s}-{!s}-{!s}-{!s}".format(attachment_metadata['last_name'],
                                                  attachment_metadata['first_name'],
                                                  attachment_metadata['person_id'],
                                                  attachment_metadata['file_name'])
    target_file = os.path.join(target_dir, attachment_uid)
    shutil.copy(attachment_path, target_file)


parser = argparse.ArgumentParser()
parser.add_argument('--server', default='127.0.0.1',
                    help='the IP or DNS Name of the MacPractice MySQL Server.')
parser.add_argument('--username', help='provide a MacPractice DB username.')
parser.add_argument('--password', help='provide a MacPractice DB password.')
parser.add_argument('--database', default='macpractice',
                    help='the name of the MacPractice database.')
parser.add_argument('--dry-run', default='macpractice', dest='dry_run',
                    help='set dry-run True if you wish to to test processing.')
parser.add_argument('--source-dir', default='/Temp/attachments', dest='source_dir',
                    help='the directory where attachments exist.')
parser.add_argument('--target-dir', default='/Temp/attachments_processed', dest='target_dir',
                    help='the directory where attachments should be copied.')
args = parser.parse_args()

# configure logging
log_format = '%(message)s'
logging.basicConfig(level='INFO', format=log_format)

# creates a list of all files to be converted
files = os.walk(args.source_dir)

mysql_connection = mysql.connector.connect(user=args.username,
                                           password=args.password,
                                           host=args.server,
                                           database=args.database,
                                           buffered=True)

for root, directories, filenames in files:
    for filename in filenames:
        attachment_path = os.path.join(root, filename)
        logging.info('Examining File: {!s}'.format(attachment_path))
        # gets attachment metadata
        attachment_metadata = get_attachment_metadata(mysql_connection, filename)
        # moves attachment to correct directory
        if (attachment_metadata['file_name'] is not None) and (attachment_metadata['file_name'] is not None):
            process_attachment_result = process_attachment(attachment_path, attachment_metadata, args.target_dir)
