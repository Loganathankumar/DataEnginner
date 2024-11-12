import os
import pandas
import uuid

import pandas as pd

"""
Question : How can I split a large csv file (7GB) into smaller Csv files using Python|
"""

class FileSettings(object):
    def __init__(self, file_name, row_size = 100):
        self.file_name = file_name
        self.row_size = row_size


class FileSplitter(object):

    def __init__(self, file_settings):
        self.file_settings = file_settings

        if type(self.file_settings).__name__ != "FileSettings":
            raise Exception("Please Pass correct instance")

        self.df = pd.read_csv(self.file_settings.file_name, chunksize= self.file_settings.row_size)


    def run(self, directory="temp"):

        try:os.makedirs(directory):
        except Exception as e:pass

        counter = 0

        while True:
            try:
                file_name = 

