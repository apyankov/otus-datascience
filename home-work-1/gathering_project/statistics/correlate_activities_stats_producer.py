from statistics.stats_producer import StatsProducer
import pandas as pd


class ActivityStatsProducer(StatsProducer):
    question = """
    Вопрос N2
    Существует ли коррелиция между разными проявлениями активности пользователей?
    """

    fields = ['comments_count', 'likes_count', 'reposts_count', 'views_count']

    def print_stats(self, df):
        self.print_question(self.question)

        with pd.option_context('display.max_columns', 10):
            print(df.corr())
