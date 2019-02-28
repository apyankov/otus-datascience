import os
import io
import json

from storages.storage import Storage


class JsonFileStorage(Storage):

    def __init__(self, file_path):
        self.file_path = file_path

    # вычитать json из файла
    def read_data(self):
        if not os.path.exists(self.file_path):
            raise StopIteration

        with open(self.file_path) as f:
            return json.load(f)

    # записать json в файл
    def write_data(self, json_data):
        """
        :param json_data: json-value to be written to file
        """
        with io.open(self.file_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(json_data, ensure_ascii=False, sort_keys=True, indent=4))

    # добавить json в файл - пока не используем
    def append_data(self, data):
        """
        :param data: json to be appended to file
        """
        raise NotImplementedError
