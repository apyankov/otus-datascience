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
        """
        Получить storage для каждого сообщества
        :return: список из ScrappedGroupData
        """
        # получить список файлов в folder, оставить только те, которые подходят под messages_file_regexp
        # названия файлов со scrapped данными
        from os import listdir
        from os.path import isfile, join

        scrapped_all_files = [f for f in listdir(self.folder) if isfile(join(self.folder_prefix, f))]
        # только те файлы, которые про сообщения на стене сообщества
        result = []
        for file_name in scrapped_all_files:
            if self.REGEXP_MESSAGES_FILE.search(file_name):
                group_id = self.REGEXP_MESSAGES_FILE.match(file_name).group(1)
                group_data = ScrappedGroupData(group_id, self.obtain_message_storage(group_id))
                result.append(group_data)
        return result

    # # из файла со scrapped_data получаем строки для csv
    # def produce_csv_rows_for_file(file_name, fields_list):
    #     group_id = SCRAPPED_DATA_FILE_REGEXP.match(file_name).group(2)
    #     result = []
    #     with io.open(join(FOLDER_PREFIX, file_name), 'r', encoding='utf-8') as f:
    #         fileContent = json.load(f)
    #         for doc in fileContent:
    #             row = [group_id] + AllGroupsParser.produce_line(doc, fields_list)
    #             result.append(row)
    #     return result
    #
    # @staticmethod
    # # для всех перечисленных файлов - записываем получаемые строки в csv (в памяти держим строки только для одного файла)
    # def write_csv_lines_by_files(csv_writer, file_name_list, fields_list):
    #     for fileName in file_name_list:
    #         print('fileName: ' + fileName)
    #         rows = AllGroupsParser.produce_csv_rows_for_file(fileName, fields_list)
    #         for row in rows:
    #             csv_writer.writerow(row)

    def obtain_csv_data_storage(self, csv_fields):
        return CsvFileStorage(self.file_path_func(self.CSV_FILE_NAME), csv_fields)

    # путь к файлу
    def file_path_func(self, file_name):
        return self.folder_prefix + file_name


class ScrappedGroupData(object):
    """
    group_id - id сообщества
    messages - json-array с сообщениями
    """

    def __init__(self, group_id, storage):
        self.group_id = group_id
        self.storage = storage
