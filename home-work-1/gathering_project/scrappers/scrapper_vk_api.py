import requests
import io
import json, codecs
import logging
import time
import vk_api

logger = logging.getLogger(__name__)

# import sys
# print(sys.path)

class ScrapperVkApi(object):
    # константы и правила, которые используем при работе с файлами
    FOLDER_PREFIX = './scrapped_data/'  # folder, в которую складываем файлы
    FILE_EXTENSION = '.txt'  # расширение для json-файлов

    def __init__(self, skip_objects=None):
        self.skip_objects = skip_objects

    def fileNameFunc(self, rawFileName):
        return self.FOLDER_PREFIX + rawFileName + self.FILE_EXTENSION

    # Данные по сообществам - получаем по http
    def obtainGroups(self, api, keyword, count):
        print("obtainGroups, keyword=" + keyword + ", count=" + str(count))
        results = api.groups.search(q=keyword, count=count)
        return results['items']

    # получить N записей со стены сообщества/пользователя
    def obtainWallItems(self, api, owner_id, count):
        # protection: проверяем входные значения
        if(count > 100): # todo: можно приделать scroll, то есть, разбить запрос на несколько, в каждом count не более 100
            raise AssertionError("count должен быть <= 100, но передали:" + str(count))

        # основная часть
        results = api.wall.get(owner_id=owner_id, filter = 'owner', count = count, offset=0)
        print("obtainWallItems, owner_id=" + str(owner_id) + ", retrieve " + str(len(results['items'])) + ", of total " + str(results['count']))
        return results['items']

        # для напоминания - сохраним в комментариях альтернативный метод vk_api для получения сообщений со стены сообщества
        #tools = vk_api.VkTools(vk_session)
        #wall = tools.get_all('wall.get', 10, {'owner_id': -92718200, 'filter':'owner'})

    # записываем json в файл
    def writeJsonToFile(self, fileName, jsonData):
        with io.open(fileName, 'w', encoding='utf-8') as f:
            f.write(json.dumps(jsonData, ensure_ascii=False, sort_keys=True, indent=4))

    def scrap_process(self, storage):
        # credentials: нужно указать логин/пароль к vk.com
        # чтобы случайно не закомитить версию с логин/пароль - выносим эти данные в отдельный файл, который исключаем в .gitignore
        credentialsFileName = "vk_login_password.txt" # формат файла: 1-я строка = логин, 2-я строка = пароль
        # затираем значения, которые могли сохраниться с прошлых запусков (в notebook)
        login = '+71231234567'
        password = ''

        # загружаем значения из файла
        try:
            loginFile = open(credentialsFileName, 'r')
            login = loginFile.readline().strip() # в формате: '+71231234567'
            password = loginFile.readline().strip()
        except IOError:
            print("No file with login/password: создайте файл " + credentialsFileName + ", в котором 1-я строка = логин, 2-я строка = пароль")

        # используем:
        #  wrapper-lib vk_api: https://github.com/python273/vk_api
        #  описание Api от vk: https://vk.com/dev/groups.search?params[q]=bitcoin&params[future]=0&params[market]=0&params[offset]=3&params[count]=3&params[v]=5.92

        # параметры запуска
        GROUP_KEYWORD = 'bitcoin'  # интересуют сообщества, в названии которых есть такое слово
        COUNT_GROUPS = 2  # обработаем столько сообществ
        COUNT_MESSAGES_PER_GROUP = 5  # в каждом сообществе, со стены возьмем столько сообщений

        # получаем соединение, api
        vk_session = vk_api.VkApi(login, password)
        vk_session.auth(token_only=True)
        api = vk_session.get_api()

        # получаем все интересующие сообщества, записываем в файл
        groups = self.obtainGroups(api, 'bitcoin', COUNT_GROUPS)
        self.writeJsonToFile(self.fileNameFunc('groups'), groups)

        # отфильтруем сообщества - нам нужны только те, у которых есть доступ к сообщениям на стене
        group_ids = []
        for group in groups:
            if group['is_closed'] == 0:
                group_ids.append(group['id'])
            else:
                print('skip group:' + str(group['id']))
        print('total groups для обработки: ' + str(len(group_ids)))

        # для каждого сообщества - получаем сообщения со стены и записваем в соотв.файл
        counter = 1  # счетчик, используем в названии файлов
        for group_id in group_ids:
            vk_id = -1 * group_id  # vk_api.wall.get по наличию знака минус - определяет, что требуется именно сообщество
            items = self.obtainWallItems(api=api, owner_id=vk_id, count=COUNT_MESSAGES_PER_GROUP)
            rawFileName = str(counter) + "_" + str(group_id)
            self.writeJsonToFile(self.fileNameFunc(rawFileName), items)
            counter = counter + 1
            time.sleep(0.5)  # соблюдаем этикет





        # You can iterate over ids, or get list of objects
        # from any API, or iterate through pages of any site
        # Do not forget to skip already gathered data
        # Here is an example for you
        url = 'https://otus.ru/'
        response = requests.get(url, cert=False)

        if not response.ok:
            logger.error(response.text)
            # then continue process, or retry, or fix your code

        else:
            # Note: here json can be used as response.json
            data = response.text

            # save scrapped objects here
            # you can save url to identify already scrapped objects
            storage.write_data([url + '\t' + data.replace('\n', '')])
