import re

from storages.csv_file_storage import CsvFileStorage
from storages.json_file_storage import JsonFileStorage


class AppDataStorageFactory(object):
    GROUP_FILE_NAME = 'groups.txt'
    PREFIX_MESSAGES_FILE = 'messages_'
    REGEXP_MESSAGES_FILE = re.compile(r'^{}([\d]+).*$'.format(PREFIX_MESSAGES_FILE))
    CSV_FILE_NAME = 'data.csv'

    def __init__(self, folder):
        self.folder = folder
        self.folder_prefix = folder + '/'
        self.messages_file_regexp = re.compile(r'^messages_([\d]+).*$')

    def obtain_group_storage(self):
        return JsonFileStorage(self.file_path_func(self.GROUP_FILE_NAME))

    def obtain_message_storage(self, group_id):
        file_name = 'messages_' + str(group_id) + '.txt'
        return JsonFileStorage(self.file_path_func(file_name))

    def obtain_all_message_storages(self):
        # получить список файлов в folder, оставить только те, которые подходят под messages_file_regexp
        raise NotImplementedError

    def obtain_csv_data_storage(self, csv_fields):
        return CsvFileStorage(self.file_path_func(self.CSV_FILE_NAME), csv_fields)

    # путь к файлу
    def file_path_func(self, file_name):
        return self.folder_prefix + file_name
