#!/usr/bin/env python3

import argparse
import ast
import csv
import json
import os
import os.path
import sys
import uuid

import requests

ALLOWED_TYPES = ("string", "integer", "double", "currency", "date", "time", "datetime", "boolean", "geo_shape", "geo_point")

AUTO_PRIMARY_KEY = '_AUTO_'


class CsvExtractor:
    def __init__(self, filename, primary_key, fields=None):
        self.filename = filename
        self.primary_key = primary_key
        exception = None
        if fields:
            self.fields = [field['field'] for field in fields]
        else:
            self.fields = None
        # Try the file with each of these encoding to see what works
        for encoding in ['utf-8', 'iso-8859-1', 'ascii']:
            try:
                with open(self.filename, encoding=encoding) as csv_file:
                    # Read in the whole file to ensure we got the right encoding
                    csv_reader = csv.DictReader(csv_file, fieldnames=self.fields)
                    for _ in csv_reader:
                        pass
                # It worked, so save this encoding
                self.encoding = encoding
                if encoding != 'utf-8':
                    print('Using', encoding, 'encoding')
                # Save the field names with . replaced by _ to make elasticsearch happy
                self.fieldnames = [x.strip().replace('.', '_') for x in csv_reader.fieldnames]
                # Mark that everything worked
                exception = None
                # And stop trying
                break
            except Exception as e:
                print('WARN: Encoding', encoding, 'failed', file=sys.stderr)
                exception = e
        # Bail if none of the encodings worked
        if exception:
            raise exception

        # Make sure that all of fieldnames are at least something
        for ii, field in enumerate(self.fieldnames):
            if not field:
                self.fieldnames[ii] = 'Column_%d' % ii

    def extract_records(self):
        with open(self.filename, encoding=self.encoding) as csv_file:
            csv_reader = csv.DictReader(csv_file, fieldnames=self.fieldnames)

            # skip the first line of the reader since it has the csv header
            next(csv_reader)

            if self.primary_key == AUTO_PRIMARY_KEY:
                for row in csv_reader:
                    row['id'] = str(uuid.uuid4())
                    yield row
            else:
                for row in csv_reader:      
                    yield row

class FieldError(Exception):
    """
    Invalid Field definitions
    """

def validate_fields(fields):
    errors = []
    for i, field in enumerate(fields):
        if 'field' not in field:
            errors.append("Missing 'field' attribute from field {}".format(i))
        if 'type' not in field:
            errors.append("Missing 'type' attribute from field {}".format(i))
        elif field['type'] not in ALLOWED_TYPES:
            errors.append('Invalid type {} for field {}.  Must be one of "{}"'.format(field['type'], i, '", "'.join(ALLOWED_TYPES)))

    if errors:
        msg = '\n'.join(errors)
        raise FieldError(msg)


def post_to_api(url, data, auth_id):
    headers = {
        'Authorization': auth_id,
    }
    r = requests.post(url, headers=headers, json=data)
    if r.status_code != requests.codes.ok:
        if response == "<Response [400]>" or response == "<Response [200]>" or response == "<Response [300]>" or response == "<Response [500]>": # response != "<Response [503]>" was not working.
            print(r.text, file=sys.stderr)
            r.raise_for_status()
        else: #Assume response 503, server busy:
            print("Error 503: server busy.  Waiting 5 seconds to retry.")
            time.sleep(5)
            post_to_api(url, data, auth_id) 
    return r.json()


def process_batch(args, rows):
    batch = {
        'datasetId': args.datasetId,
        'rows': rows,
        'index': not args.noIndex,
    }
    post_to_api(args.server + '/dataset/updaterows', batch, args.apiKey)


def process_file(args):
    filename = os.path.expanduser(args.filename)
    if args.fields:
        with open(os.path.expanduser(args.fields)) as fields_file:
            fields = json.load(fields_file)
            validate_fields(fields)
    else:
        fields = None
    csv_reader = CsvExtractor(filename, args.primaryKey, fields)
    batches = 1
    if not args.datasetId:
        if not args.folderId:
            raise ValueError('Must specify folder ID when creating a new dataset')

        print('Creating a new dataset')
        data = {
            "name": args.name if args.name else os.path.splitext(os.path.basename(filename))[0],
            "folderId": args.folderId,
            "primaryKey": args.primaryKey,
            "description": "Uploaded using CSV uploader example script",
        }

        if not fields:
            fields = []
            for f in csv_reader.fieldnames:
                fields.append({
                    "field": f,
                    'displayName': f,
                    'autocomplete': False,
                    "type": 'string',
                })

        # If there's a primary key
        if args.primaryKey != AUTO_PRIMARY_KEY:
            if args.primaryKey not in csv_reader.fieldnames:
                raise ValueError('Primary key ({}) does not appear in {}'.format(args.primaryKey, filename))
        else:
            # Otherwise, set id as the primary key
            data["primaryKey"] = 'id'
            # And add the field for it
            fields.append({
                "field": "id",
                'displayName': "id",
                'autocomplete': False,
                "type": 'string',
            })

        data['fields'] = fields

        res = post_to_api(args.server + '/dataset/create', data, args.apiKey)
        args.datasetId = res['datasetId']
        print('Dataset ID: {}'.format(res['datasetId']))
    
    elif fields:
        # we have a datasetId and field defs.  Update the dataset to use the field defs provided
        data = {
            'datasetId': args.datasetId,
            'fields': fields
        }
        res = post_to_api(args.server + '/dataset/updatefields', data, args.apiKey)
        print('Dataset Fields Updated: {success}'.format(**res))
    if args.clear:
        print('Clearing all Rows in dataset %s' % args.datasetId)
        post_to_api(args.server + '/dataset/deleteallrows', {'datasetId': args.datasetId}, args.apiKey)
    print('Updating Rows in dataset %s' % args.datasetId)
    rows = []
    for record in csv_reader.extract_records():
        for field_name, value in record.items():
            try:
                value_array = ast.literal_eval(value)
                if isinstance(value_array, list):
                    if len(value_array) > 1:
                        record[field_name] = value_array
                    elif value_array:
                        record[field_name] = value_array[0]
                    else:
                        record[field_name] = None
            except (SyntaxError, ValueError):
                pass
        rows.append(record)
        
        if len(rows) >= args.batchSize:
            process_batch(args, rows)
            rows = []
            rows.clear()
            batches += 1

    if rows:
        process_batch(args, rows)

    print("Uploaded file in {} batches".format(batches))

    if args.noIndex:
        print('Sending index request')
        post_to_api(args.server + '/dataset/index', {'datasetId': args.datasetId}, args.apiKey)


def main():
    parser = argparse.ArgumentParser(description='A utility to upload csv files to Numetric using the API.')
    # Add the arguments for the input
    input_parser = parser.add_argument_group('input arguments')
    input_parser.add_argument('-i', '--filename', required=True)
    # Add the arguments for the server to connect to
    server_parser = parser.add_argument_group('server arguments')
    server_parser.add_argument('-s', '--server', help='The server to upload to', default='http://cloud-dev.numetric.com:3002')
    server_parser.add_argument('-k', '--apiKey', required=True)
    # The arguments for the dataset to create
    dataset_parser = parser.add_argument_group('dataset arguments')
    dataset_parser.add_argument('-d', '--datasetId')
    dataset_parser.add_argument('-f', '--folderId')
    dataset_parser.add_argument('-n', '--name')
    dataset_parser.add_argument('-p', '--primaryKey',
                                help='The primary key field ({} for auto-generated UUIDs)'.format(AUTO_PRIMARY_KEY),
                                required=True)
    dataset_parser.add_argument('-b', '--batchSize', help='Number of rows to send in each batch', default=3000, type=int)                            
    dataset_parser.add_argument('-x', '--noIndex', action='store_true',
                                help="Don't perform incremental indexing (index after upload completes)")
    dataset_parser.add_argument('-c', '--clear', action='store_true',
                                help="Clear all rows from dataset before uploading")                            
    dataset_parser.add_argument('-j', '--fields', action='store',
                                help='The path the a json file with field definitions. '
                                     'Field definitions must match those acceptable to the Numetric API: '
                                     'https://numetric-api.readme.io/docs/field-definition')
    # And the optional arguments
    parser.add_argument('-l', '--log', action='store_true', help='Enable logging on the server')

    args = parser.parse_args()

    try:
        process_file(args)
    except requests.exceptions.RequestException as e:
        print("Error uploading {}: {}".format(args.filename, e), file=sys.stderr)
    except Exception as e:
        print("Error reading {}: {}".format(args.filename, e), file=sys.stderr)

if __name__ == '__main__':
    main()