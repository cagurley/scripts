# -*- coding: utf-8 -*-
"""
Created on Sat Sep 22 17:18:31 2018

@author: Atticus
"""

import os


def seq_ren_files(dirpath, start_num=1, num_digits=2, prefix=''):
    """
    Renames files in dirpath as a sequence of prefix with a numeric counter of
    length num_digits beginning at start_num.
    """
    try:
        if isinstance(os.altsep, str):
            dirpath = dirpath.rstrip(r'\/').replace(os.altsep, os.sep)
            parentdir = os.path.join(dirpath, '..')
        if (
            not os.path.exists(dirpath)
            or (
                os.path.exists(parentdir)
                and os.path.split(dirpath)[1] not in os.listdir(parentdir)
            )
            or not os.access(dirpath, os.W_OK)
        ):
            raise ValueError("'{}': Path does not exist or cannot write to directory.".format(dirpath))
    except ValueError as e:
        print(str(e))
    else:
        filepaths = []
        index = 0
        with os.scandir(dirpath) as contents:
            for item in contents:
                if os.access(item, os.W_OK) and item.is_file():
                    filepaths.append(item.path)
        for file in filepaths:
            counter = str(start_num + index)
            while len(counter) < num_digits:
                counter = '0' + counter
            extension = file.rsplit('.', 1)[1]
            new_name = str(prefix) + counter + '.' + extension
            new_path = os.path.join(dirpath, new_name)
            temp_name = new_name
            if file != new_path and os.access(new_path, os.F_OK):
                while os.access(os.path.join(dirpath, temp_name), os.F_OK):
                    temp_name = 'TEMP' + temp_name
                try:
                    os.replace(new_path, os.path.join(dirpath, temp_name))
                    for i, filepath in enumerate(filepaths):
                        if filepath == new_path:
                            filepaths[i] = os.path.join(dirpath, temp_name)
                            break
                except OSError as e:
                    print(str(e))
            os.replace(file, new_path)
            index += 1
        print("'{}': Files successfully renamed.".format(dirpath))
        return None
