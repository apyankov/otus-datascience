import abc


class StatsProducer(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def print_stats(self, df):
        """
        Вычисляет некоторые статистики и печатает в консоль
        :param df: dataframe
        """
        raise NotImplementedError
