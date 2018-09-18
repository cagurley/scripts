# -*- coding: utf-8 -*-
"""
Created on Tue Aug 21 16:46:59 2018

@author: cagurl01
"""

import csv
import datetime as dt
import os


def split_visits(filepath, file_date):
    """
    Provide filepath and file date as 'MMDDYYYY'
    """
    try:
        if len(file_date) != 8 or not file_date.isdigit():
            raise ValueError("File date must be of form 'MMDDYYYY'")
    except ValueError as e:
        print(str(e))
    else:
        # Declare variables needed
        filepath = filepath.replace('\\', '/')
        split_path = filepath.rsplit('/', 1)
        dirpath = ''
        if len(split_path) == 2:
            dirpath = split_path[0] + '/'
        elif len(split_path) == 1:
            dirpath = ''
        else:
            print('Error in provided filepath')
            return None
        file_date = dt.datetime.strptime(file_date, '%m%d%Y')
        all_rows = []
        header = []
        attend_rows = []
        not_attend_rows = []
        event_meeting_pairs = set()
        sorted_rows = []
        not_attend_filepath = (
            dirpath
            + 'vis_update_not_attended_for_heather_'
            + file_date.strftime('%m%d%y')
            + '.csv'
        )
        if os.access(not_attend_filepath, os.F_OK):
            warning = input('Output files already exist. Continue to overwrite?  ').lower()
            if warning in ('n', 'no'):
                print('Process aborted!')
                return None
        # Write raw rows to object
        with open(filepath, newline='') as file:
            reader = csv.reader(file)
            for row in reader:
                all_rows.append(row)
        # Split rows into full header, not attend full rows, and attend short rows
        header = all_rows.pop(0)
        for row in all_rows:
            event_nbr = row[2].strip()
            meeting_nbr = row[3].strip()
            event_date = dt.datetime.strptime(str(row[4].strip().split()[0]), '%m/%d/%Y')
            emplid = row[6].strip()
            attendance = None
            if row[19].strip().lower() == 'yes':
                attendance = True
            elif row[19].strip().lower() == 'no':
                attendance = False
            short_row = [event_nbr, meeting_nbr, event_date, emplid, attendance]
            if short_row[2] < file_date and short_row[4] is False:
                not_attend_rows.append(row)
            else:
                attend_rows.append(short_row)
        # Write header and not attend rows to file
        with open(not_attend_filepath, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            writer.writerows(not_attend_rows)
        # Find unique event/meeting pairs
        for row in attend_rows:
            if (row[0], row[1]) not in event_meeting_pairs and row[0] != '' and row[1] != '':
                event_meeting_pairs.add((row[0], row[1]))
        # Convert set of tuples of event/meeting strings to list of lists of event/meeting integers and sort
        event_meeting_pairs = list(event_meeting_pairs)
        for index, pair in enumerate(event_meeting_pairs):
            pair = list(pair)
            for pair_index, nbr in enumerate(pair):
                pair[pair_index] = int(nbr)
            event_meeting_pairs[index] = pair
        event_meeting_pairs.sort()
        # Revert integers to strings
        for index, pair in enumerate(event_meeting_pairs):
            for pair_index, nbr in enumerate(pair):
                pair[pair_index] = str(nbr)
            event_meeting_pairs[index] = pair
        # Sort attend rows by event/meeting pair
        for pair in event_meeting_pairs:
            attendees = []
            for row in attend_rows:
                if pair[0] == row[0] and pair[1] == row[1]:
                    attendees.append([row[3]])
            sorted_rows.append([pair, attendees])
        # Write each sorted row block to its own file
        for event_file in sorted_rows:
            with open(dirpath + 'vis_' + event_file[0][0] + '.' + event_file[0][1] + '.attend_' + file_date.strftime('%m%d%y') + '.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(event_file[1])
        return None
