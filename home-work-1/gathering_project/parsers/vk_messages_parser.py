from parsers.parser import Parser


class VkMessagesParser(Parser):

    def __init__(self, group_id, fields):
        """
        :param group_id: id сообщества vk
        :param fields: список из key-value, где key - значение столбца в итоговом dataframe, value - путь, к значению в исходных данных
        """
        super().__init__(fields)
        self.group_id = group_id

    def parse(self, messages_array):
        """
        Parses scrapped vk-messages for group
        :param messages_array: json-array of vk-messages
        :return: dataframe, representing data received
        """
        # Your code here
        result = []
        for message in messages_array:
            result.append(self.produce_line(self.group_id, message, self.fields))

        return result

    # из объекта - получаем строку для csv
    def produce_line(self, group_id, json_data, fields_list):
        result = [group_id]
        for csv_field, json_path in fields_list.items():
            data = self.reveal_field_value(json_data, json_path)
            result.append(data)
        return result

    # получаем значение поля в json
    def reveal_field_value(self, json_data, field_path):
        if self.has_field_path_function(field_path):
            return self.reveal_special_value(json_data, field_path)
        else:
            return self.reveal_raw_value(json_data, field_path)

    @staticmethod
    def reveal_raw_value(json_data, field_path):
        none_value = ''
        path = field_path.split('.')
        data = json_data
        for step in path[:-1]:
            if step in data:
                data = data[step]
            else:
                return none_value
        data = data.get(path[-1], none_value)
        return data

    # если field_path содержит указание функции - вернет True
    @staticmethod
    def has_field_path_function(field_path):
        return len(field_path.split('?')) > 1

    # получение значения для поля, в котором указана функция
    def reveal_special_value(self, json_data, field_path):
        assert self.has_field_path_function(field_path)
        tmp = field_path.split('?')
        value_path = tmp[0]
        func_code = tmp[1]
        value = self.reveal_field_value(json_data, value_path)
        return self.apply_func(func_code, value)

    @staticmethod
    def apply_func(func_code, value, ):
        """
        Применяем функцию к значению
        :param func_code: string-code для функции
        :return: результат применения функции к value
        """
        if func_code == 'len':
            return len(value)
        else:
            raise AssertionError('unknown func_code: ' + str(func_code))


class AllGroupsParser(object):
    # разделитель в json_path - переход к св-ву
    FIELD_PATH_DIVIDER = "."
    # разделитель в json_path - применение функции
    FIELD_PATH_FUNC_DIVIDER = "/"

    """
    поля, которые интересуют
    формат: название в csv -> json_path, то есть, как получить из json
    json_path: '.' - путь во вложенном dict, '?' - после этого знака можем указать функцию, которую применить
    """
    FIELDS_OF_INTEREST = {
        'date': 'date',
        'comments_count': 'comments.count',
        'likes_count': 'likes.count',
        'reposts_count': 'reposts.count',
        'views_count': 'views.count',
        'text_len': 'text?len'  # в notebook функции не применяли
    }
    # в обработанном виде
    csv_fields = []
    json_fields = []

    def __init__(self, storage_factory):
        self.storage_factory = storage_factory
        for key, value in self.FIELDS_OF_INTEREST.items():
            self.csv_fields.append(key)
            self.json_fields.append(value)

    def transform_all_groups(self):
        """
        Проводим обработку всех scrapped_data
        :return: ссылку на cvs_storage, в котором сохранены результаты обработки
        """
        # 1. пройти по всем файлам, которые с названием [N]_[id].txt
        # 2. Составляем csv-файл со всеми сообщениями, сохраняем
        storage_list = self.storage_factory.obtain_all_message_storages()
        result_storage = self.storage_factory.obtain_csv_data_storage(['group_id'] + self.csv_fields)
        for index, scrapped_group_data in enumerate(storage_list):
            parser = VkMessagesParser(scrapped_group_data.group_id, self.FIELDS_OF_INTEREST)
            parsed_df = parser.parse(scrapped_group_data.storage.read_data())

            if index == 0:  # для первого сообщества - используем write, чтобы проставить названия столбцов
                result_storage.write_data(parsed_df)
            else:
                result_storage.append_data(parsed_df)
        return result_storage
