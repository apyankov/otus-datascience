import unittest

from storages.app_data_storage import AppDataStorageFactory


class TestJsonStorage(unittest.TestCase):
    SAMPLE_DATA = {'a':1, 'b': 2}

    def test_1(self):
        factory = AppDataStorageFactory('./test_folder')

        group_storage = factory.obtain_group_storage()
        group_storage.write_data(self.SAMPLE_DATA)
        restored_data = group_storage.read_data()
        # self.assertEqual(len(restored_data), 1)
        self.assertDictEqual(restored_data, self.SAMPLE_DATA)


class TestHtmlParser(unittest.TestCase):

    def test_parse(self):
        # Your code here
        pass


if __name__ == '__main__':
    unittest.main()
