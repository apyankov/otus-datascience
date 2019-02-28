import requests
import io
import json, codecs
import logging
import time
import vk_api

logger = logging.getLogger(__name__)


# import sys
# print(sys.path)


class Credentials(object):
    """
    Хранит логин, пароль
    """

    def __init__(self, login, password):
        self.login = login
        self.password = password


class ScrapperVkApi(object):
    # константы и правила, которые используем при работе с файлами
    FOLDER_PREFIX = './scrapped_data/'  # folder, в которую складываем файлы
    FILE_EXTENSION = '.txt'  # расширение для json-файлов

    def __init__(self, storage_factory, skip_objects=None):
        self.storage_factory = storage_factory
        self.skip_objects = skip_objects

    def file_name_func(self, raw_file_name):
        return self.FOLDER_PREFIX + raw_file_name + self.FILE_EXTENSION

    # Данные по сообществам - получаем по http
    def scrap_groups(self, api, keyword, count):
        print("scrap_groups, keyword=" + keyword + ", count=" + str(count))
        results = api.groups.search(q=keyword, count=count)
        return results['items']

    # получить N записей со стены сообщества/пользователя
    def obtainWallItems(self, api, owner_id, count):
        # protection: проверяем входные значения
        if (
                count > 100):  # todo: можно приделать scroll, то есть, разбить запрос на несколько, в каждом count не более 100
            raise AssertionError("count должен быть <= 100, но передали:" + str(count))

        # основная часть
        results = api.wall.get(owner_id=owner_id, filter='owner', count=count, offset=0)
        print("obtainWallItems, owner_id=" + str(owner_id) + ", retrieve " + str(
            len(results['items'])) + ", of total " + str(results['count']))
        return results['items']

        # для напоминания - сохраним в комментариях альтернативный метод vk_api для получения сообщений со стены сообщества
        # tools = vk_api.VkTools(vk_session)
        # wall = tools.get_all('wall.get', 10, {'owner_id': -92718200, 'filter':'owner'})

    # записываем json в файл
    def writeJsonToFile(self, fileName, jsonData):
        with io.open(fileName, 'w', encoding='utf-8') as f:
            f.write(json.dumps(jsonData, ensure_ascii=False, sort_keys=True, indent=4))

    def obtain_credentials(self, credentials_file_path):
        """
        чтобы случайно не закомитить версию с логин/пароль - эти данные храним в отдельном файле, который исключаем в .gitignore
        :param credentials_file_path: путь к файлу, в котором: 1-я строка = логин (в формате: '+71231234567'), 2-я строка = пароль
        :return: объект Credentials
        :raises: IOError - если файл не найден, либо не удалось считать логин, пароль
        """

        # загружаем значения из файла
        try:
            credentials_file = open(credentials_file_path, 'r')
            login = credentials_file.readline().strip()
            password = credentials_file.readline().strip()
            return Credentials(login, password)
        except IOError as exception:
            print(
                "No file with login/password: создайте файл " + credentials_file_path + ", в котором 1-я строка = логин, 2-я строка = пароль")
            raise exception

    @staticmethod
    def obtain_vk_api(login, password):
        # получаем соединение, api
        vk_session = vk_api.VkApi(login, password)
        vk_session.auth(token_only=True)
        return vk_session.get_api()

    def reveal_group_ids_of_interest(self, groups):
        """
        отфильтруем сообщества - нам нужны только те, у которых есть доступ к сообщениям на стене
        :param groups: список json, для групп, полученных из vk_api
        :return: список id сообществ, которые нам нужны
        """
        group_ids = []
        for group in groups:
            if group['is_closed'] == 0:
                group_ids.append(group['id'])
            else:
                print('skip group:' + str(group['id']))
        print('total groups для обработки: ' + str(len(group_ids)))
        return group_ids

    def scrap_process(self, credentials_file_path, group_keyword, count_groups, count_messages_per_group):
        """
        собираем данные из vk.api
        :param credentials_file_path: путь к файлу с логин, пароль
        :param group_keyword: интересуют сообщества, в названии которых есть такое слово
        :param count_groups: обработаем столько сообществ
        :param count_messages_per_group: в каждом сообществе, со стены возьмем столько сообщений
        :return: void
        """

        credentials = self.obtain_credentials(credentials_file_path)

        # используем:
        #  wrapper-lib vk_api: https://github.com/python273/vk_api
        #  описание Api от vk: https://vk.com/dev/groups.search?params[q]=bitcoin&params[future]=0&params[market]=0&params[offset]=3&params[count]=3&params[v]=5.92
        api_vk = self.obtain_vk_api(credentials.login, credentials.password)

        # получаем все интересующие сообщества, записываем в файл
        groups = self.scrap_groups(api_vk, group_keyword, count_groups)
        self.storage_factory.obtain_group_storage().write_data(groups)  # запишем полученные сообщества в storage

        # отфильтруем сообщества - нам нужны только те, у которых есть доступ к сообщениям на стене
        group_ids = self.reveal_group_ids_of_interest(groups)

        # для каждого сообщества - получаем сообщения со стены и записываем в соотв.файл
        for group_id in group_ids:
            vk_id = -1 * group_id  # vk_api.wall.get по наличию знака минус - определяет, что требуется именно сообщество
            items = self.obtainWallItems(api=api_vk, owner_id=vk_id, count=count_messages_per_group)
            self.storage_factory.obtain_message_storage(group_id).write_data(items)
            time.sleep(0.5)  # соблюдаем этикет
