#!/usr/bin/env python3

import csv
import os
import os.path
import sys
from collections import defaultdict

# Allow individual fields be up to 1 MB
csv.field_size_limit(1 << 20)


class CsvExtractor:
    def __init__(self, filename):
        self.filename = filename
        exception = None
        # Try the file with each of these encoding to see what works
        for encoding in ['utf-8-sig', 'utf-8', 'iso-8859-1', 'ascii']:
            try:
                with open(self.filename, encoding=encoding) as csv_file:
                    # Read in the whole file to ensure we got the right encoding
                    csv_reader = csv.DictReader(csv_file)
                    for _ in csv_reader:
                        pass
                # It worked, so save this encoding
                self.encoding = encoding
                if encoding != 'utf-8':
                    print('Using', encoding, 'encoding')
                # Save the field names with . replaced by _ to make elasticsearch happy
                self.fieldnames = [x.strip().replace('.', '_') for x in csv_reader.fieldnames]
                # Move _ to the end of the field name to make API happy
                for i, field_name in enumerate(self.fieldnames):
                    if field_name.startswith('_'):
                        print("Moving _ to the end for {}".format(field_name), file=sys.stderr)
                        self.fieldnames[i] = field_name[1:] + "_"
                # Mark that everything worked
                exception = None
                # And stop trying
                break
            except UnicodeDecodeError as e:
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

            for row in csv_reader:
                yield row


def process_file(filename):
    filename = os.path.expanduser(filename)
    csv_reader = CsvExtractor(filename)
    uniques = defaultdict(lambda: defaultdict(int))
    total = 0
    for record in csv_reader.extract_records():
        for k, v in record.items():
            uniques[k][v] += 1

        total += 1

    print("Total: {}".format(total))
    count = {k: len(v) for k, v in uniques.items()}
    for k, v in sorted(count.items(), key=lambda x: x[1], reverse=True):
        print("{}: {}".format(k, v))


def main():
    if len(sys.argv) < 1:
        print("usage: {} <filename>".format(sys.argv[0]))
    process_file(sys.argv[1])

if __name__ == '__main__':
    main()
