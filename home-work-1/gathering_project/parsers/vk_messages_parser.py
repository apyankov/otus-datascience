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

    @staticmethod
    # из объекта - получаем строку для csv
    def produce_line(group_id, json_data, fields_list):
        result = [group_id]
        for field in fields_list:
            data = VkMessagesParser.reveal_field_value(json_data, field)
            result.append(data)
        return result

    @staticmethod
    # получаем значение поля в json
    def reveal_field_value(json_data, field_path):
        none_value = ''
        path = field_path.split('.')
        data = json_data
        for step in path[:-1]:
            if step in data:
                data = data[step]
            else:
                return none_value
        data = data.get(path[-1], none_value)
        # print(data)
        return data


class AllGroupsParser(object):
    # поля, которые интересуют
    FIELDS_OF_INTEREST = {
        'comments_count': 'comments.count',
        'date': 'date',
        'likes_count': 'likes.count',
        'marked_as_ads': 'marked_as_ads',
        'post_source': 'post_source.type',
        'reposts_count': 'reposts.count',
        #'text': 'text',
        'views_count': 'views.count'
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
