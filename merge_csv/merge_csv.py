# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import csv
import os


class File:
    """
    Provide CSV filename sans extension
    and optional header count as a positive integer (defaulted to zero).
    """
    def __init__(self, name, header_count=0):
        self.name = name
        self.header_count = header_count

    def __str__(self):
        return "CSV file declaration with name '{self.name}' and {self.header_count} header rows.".format(self=self)


def merge_csv(dirpath, files, new_filename):
    """
    Provide directory path, a list of File objects
    in the same directory to merge, and the name for the new, merged file.
    """
    try:
        if isinstance(os.altsep, str):
            dirpath = dirpath.rstrip(r'\/').replace(os.altsep, os.sep)
        new_filepath = os.path.join(dirpath, new_filename + '.csv')
        file_tuples = []
        if (
            not os.path.exists(dirpath)
            or not os.access(dirpath, os.W_OK)
        ):
            raise ValueError("'{}': Directory path does not exist or cannot write to directory.".format(dirpath))
#        if not os.access(new_filepath, os.W_OK):
#            raise ValueError("'{}': Cannot write to new file.".format(new_filename + '.csv'))
        for index, file in enumerate(files):
            if not isinstance(file, File):
                raise TypeError("files[{}]: Object is not a File instance.".format(index))
            filepath = os.path.join(dirpath, file.name + '.csv')
            if (
                not os.path.exists(filepath)
                or not os.access(filepath, os.W_OK)
            ):
                raise ValueError("'{}': File path does not exist or cannot write to file.".format(file.name + '.csv'))
            elif (
                not isinstance(file.header_count, int)
                or file.header_count < 0
            ):
                raise ValueError("'{}': File's header_count must be an non-negative integer.".format(file.name + '.csv'))
            else:
                file_tuples.append((filepath, file.name + '.csv', file.header_count))
    except TypeError as e:
        print(str(e))
    except ValueError as e:
        print(str(e))
    else:
        all_rows = []
        ignore = False
        for ftuple in file_tuples:
            if ftuple[2] >= 10 and ignore is False:
                warning = input("""
                    More than nine header rows declared. Continue?
                    Enter to continue, 'N' to abort,
                    or type 'ALL' to continue
                    and ignore subsequent warnings of this type:
                """)
                if warning.lower() in ('n', 'no'):
                    print('Merge aborted.')
                    return None
                elif warning.lower() == 'all':
                    ignore = True
                    print('Warnings are now silenced.')
            with open(ftuple[0], newline='') as file:
                file_rows = []
                reader = csv.reader(file)
                for index, row in enumerate(reader):
                    if index < ftuple[2]:
                        continue
                    else:
                        file_rows.append(row)
                if len(file_rows) == 0:
                    print("""
                        '{}': No rows extracted. File was empty
                        or declared quantity of header rows
                        was greater than or equal to file length.
                    """.format(ftuple[1]))
                else:
                    all_rows.extend(file_rows)
                    print("'{}': {} rows extracted.".format(ftuple[1], len(file_rows)))
        with open(new_filepath, 'w', newline='') as combined_file:
            writer = csv.writer(combined_file)
            writer.writerows(all_rows)
            print("'{}': New file created with {} rows.".format(new_filename + '.csv', len(all_rows)))
        return None
