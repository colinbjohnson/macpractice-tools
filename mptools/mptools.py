#!/usr/bin/env python

import logging

import mysql.connector


class Attachment:
    def __init__(self, attached_file_id):
        self.attached_file_id = attached_file_id
        self.hash = None
        self.file_name = None
        # all attributes below are extensions to the Attachment class
        self.patient_id = None
        self.attachment_type = None
        self.file_path = None
        self.image_attachment_type_id = None
        self.image_attachment_type_name = None

    def get_image_attachment_type(self, mysql_connection):
        '''
        given MySQL Connection Object and an Attachment of "Image" type, return the type of attachment
        '''
        image_attachment_type_name = None
        query = ('SELECT * FROM attachment_type WHERE attachment_type_id=\'{!s}\';'
                 .format(self.image_attachment_type_id))
        cursor = mysql_connection.cursor(dictionary=True)
        cursor.execute(query)
        image_attachment_type_row = cursor.fetchall()
        if image_attachment_type_row['image_attachment_type_id'] is not None:
            image_attachment_type_name = image_attachment_type_row['image_attachment_type_id']
        return image_attachment_type_name

    def get_attachment_type(self, mysql_connection):
        # TODO: clean up attachment_type_query
        '''
        given MySQL Connection Object and an Attachment Object, return the type of attachment
        where attachment type is either Image, Order or None
        '''
        attachment_type = None
        attachment_type_query = ('SELECT attached_file.attached_file_id AS attached_file_id, '
                                 'image_attachments.attached_file_id AS attached_file_id_image, '
                                 'order_attachment_tie.attached_file_id AS attached_file_id_order '
                                 'FROM attached_file '
                                 'LEFT JOIN image_attachments '
                                 'ON attached_file.attached_file_id=image_attachments.attached_file_id '
                                 'LEFT JOIN order_attachment_tie '
                                 'ON attached_file.attached_file_id=order_attachment_tie.attached_file_id '
                                 'where attached_file.attached_file_id=\'{!s}\';'.format(self.attached_file_id))
        attachment_type_result = get_row_result(mysql_connection, attachment_type_query)
        if attachment_type_result is None:
            logging.warn('attachment_id {!s} is of Type: Unknown.'.format(self.attached_file_id))
        else:
            if attachment_type_result['attached_file_id_image'] == self.attached_file_id:
                attachment_type = 'image'
                logging.info('attachment_id {!s} is of Type: Image.'.format(self.attached_file_id))
            elif attachment_type_result['attached_file_id_order'] == self.attached_file_id:
                attachment_type = 'order'
                logging.info('attachment_id {!s} is of Type: Order.'.format(self.attached_file_id))
            else:
                logging.warn('attachment_id {!s} is of Type: Unknown.'.format(self.attached_file_id))
        return attachment_type

    def get_patient_id(self, mysql_connection, attachment_type):
        patient_id = None
        if attachment_type == 'image':
            query = 'SELECT patient_id FROM image_attachments WHERE ' \
                    'attached_file_id=\'{!s}\';'.format(self.attached_file_id)
        elif attachment_type == 'orders':
            query = 'SELECT orders.patient_id FROM order_attachment_tie ' \
                    'LEFT JOIN orders ON order_attachment_tie.orders_id=orders.orders_id ' \
                    'WHERE order_attachment_tie.orders_id=\'{!s}\';'\
                    .format(self.attached_file_id)
        else:
            return None
        patient_id_row = get_row_result(mysql_connection, query)
        patient_id = patient_id_row['patient_id']
        return patient_id

    def get_image_attachment_type_id(self, mysql_connection):
        image_attachment_type_id = None
        if self.attachment_type == 'image':
            query = 'SELECT attachment_type_id FROM image_attachments WHERE ' \
                    'attached_file_id=\'{!s}\';'.format(self.attached_file_id)
            image_attachment_row = get_row_result(mysql_connection, query)
            image_attachment_type_id = image_attachment_row['attachment_type_id']
        else:
            logging.warn('image_attachment_type_id Not Found for: {!s}'.format(self.attached_file_id))
        return image_attachment_type_id

    def get_image_attachment_type_name(self, mysql_connection):
        image_attachment_type_name = None
        if self.image_attachment_type_id is not None:
            query = 'SELECT attachment_type FROM attachment_type WHERE ' \
                    'attachment_type_id=\'{!s}\';'.format(self.image_attachment_type_id)
            attachment_type_row = get_row_result(mysql_connection, query)
            image_attachment_type_name = attachment_type_row['attachment_type']
        else:
            logging.warn('image_attachment_type_name Not Found for: {!s}'.format(self.image_attachment_type_id))
        return image_attachment_type_name

    @classmethod
    def get_attachment_by_id(cls, mysql_connection, attachment_id):
        attachment = None
        query = 'SELECT attached_file_id, hash, file_name '\
                'FROM attached_file WHERE attached_file_id=\'{!s}\';'.format(attachment_id)
        attachment_row = get_row_result(mysql_connection, query)
        if attachment_row is not None:
            attached_file_id = attachment_row['attached_file_id']
            attachment = Attachment(attached_file_id)
            attachment.file_name = attachment_row['file_name']
            attachment.hash = attachment_row['hash']
        return attachment

    @classmethod
    def get_attachment_by_hash(cls, mysql_connection, attachment_hash):
        attachment = None
        query = 'SELECT * FROM attached_file WHERE hash=\'{!s}\';'.format(attachment_hash)
        attachment_row = get_row_result(mysql_connection, query)
        if attachment_row is not None:
            logging.info('Attachment with Hash {!s} found.'.format(attachment_hash))
            attached_file_id = attachment_row['attached_file_id']
            attachment = Attachment(attached_file_id)
            attachment.file_name = attachment_row['file_name']
            attachment.hash = attachment_row['hash']
            attachment.attachment_type_id = attachment_row['hash']

        else:
            logging.warn('Attachment with Hash {!s} not found.'.format(attachment_hash))
        return attachment

    @classmethod
    def get_all_attachments(cls, mysql_connection):
        attachments = {}
        query = 'SELECT * FROM attached_file'
        cursor = mysql_connection.cursor(dictionary=True)
        cursor.execute(query)
        all_attachment_rows = cursor.fetchall()
        for attachment_row in all_attachment_rows:
            attached_file_id = attachment_row['attached_file_id']
            attachments[attached_file_id] = Attachment(attached_file_id)
            attachments[attached_file_id].file_name = attachment_row['file_name']
            attachments[attached_file_id].hash = attachment_row['hash']
            logging.info('Got Attachment with Attached File ID: {!s}'.format(attached_file_id))
        return attachments


class Patient:
    def __init__(self, patient_id):
        self.patient_id = patient_id
        # all attributes below are extensions to the Attachment class
        self.person_id = None
        self.last = None
        self.first = None

    @classmethod
    def get_all_patients(cls, mysql_connection):
        patients = {}
        query = 'SELECT * FROM patient'
        cursor = mysql_connection.cursor(dictionary=True)
        cursor.execute(query)
        all_patient_rows = cursor.fetchall()
        for patient_row in all_patient_rows:
            patient_id = patient_row['patient_id']
            patients[patient_id] = Patient(patient_id)
            patients[patient_id].person_id = patient_row['person_id']
            logging.info('Got Patient with Patient ID: {!s}'.format(patient_id))
        return patients


class Person:
    def __init__(self, person_id):
        self.person_id = person_id
        self.last_name = None
        self.first_name = None

    @classmethod
    def get_person_by_id(cls, mysql_connection, person_id):
        person = None
        query = 'SELECT * FROM person WHERE person_id=\'{!s}\';'.format(person_id)
        person_row = get_row_result(mysql_connection, query)
        if person_row is not None:
            person_id = person_row['person_id']
            person = Person(person_id)
            person.last = person_row['last']
            person.first = person_row['first']
            logging.info('Got Person with Person ID: {!s}'.format(person_id))
        return person


def get_row_result(mysql_connection, query):
    '''
    given a MySQL Query, return the result or return None.
    '''
    cursor = mysql_connection.cursor(dictionary=True)
    cursor.execute(query)
    result = cursor.fetchall()
    if result and len(result) == 1:
        return result[0]
    else:
        return None
