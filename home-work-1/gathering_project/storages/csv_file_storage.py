import csv
import os

import pandas as pd

from storages.storage import Storage


class CsvFileStorage(Storage):

    def __init__(self, file_path, csv_fields):
        self.file_path = file_path
        self.csv_fields = csv_fields

    # вычитать dataframe из файла
    def read_data(self):
        """
        не очень корректное Api, т.к. записываем список, а на считывании получаем dataframe
        :return: dataframe
        """
        if not os.path.exists(self.file_path):
            raise StopIteration

        df = pd.read_csv(self.file_path)
        return df[self.csv_fields]

    # записать dataframe в файл
    def write_data(self, row_list):
        """
        :param row_list: list of rows (row = list of values) to be written to file
        """
        with open(self.file_path, 'w', newline='', encoding='utf-8') as csvFile:
            csv_writer = csv.writer(csvFile, delimiter=',')
            csv_writer.writerow(self.csv_fields)
            for row in row_list:  # .iterrows():
                csv_writer.writerow(row)

    # добавить в файл строки из dataframe
    def append_data(self, row_list):
        """
        :param row_list: list of rows (row = list of values) to be appended to file
        """
        with open(self.file_path, 'a', newline='', encoding='utf-8') as csvFile:
            csv_writer = csv.writer(csvFile, delimiter=',')
            for row in row_list:
                csv_writer.writerow(row)
