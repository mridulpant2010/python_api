""" write to a flat file """

import csv
import sys
import os, shutil



""" write list of dictionaries to a csv """
def write_list_to_csv(fpath, fname, input, title):
    with open(fpath+fname, 'w',newline='',encoding='utf-8') as out_file:
        cw = csv.DictWriter(out_file, title, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        cw.writeheader() #header is not required in the file outputs
        cw.writerows(input)



"""
move files from source to destination
"""
def move_files(source,destination):
    files = os.listdir(source)
    files.sort()
    for f in files:
        src = source + f
        dst = destination + f
        shutil.move(src, dst)