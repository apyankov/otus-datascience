from statistics.stats_producer import StatsProducer
import pandas as pd


class SimpleStatsProducer(StatsProducer):
    question = """
    Вопрос N1
    Показать базовую статистику по собранным метрикам
    """

    fields = df_1 = ['group_id', 'date', 'comments_count', 'likes_count', 'reposts_count', 'views_count', 'text_len']

    def print_stats(self, df):
        self.print_question(self.question)

        print('\n*** df.info() ***')
        df.info()

        print('\n*** df.describe() ***')
        with pd.option_context('display.max_columns', 10, 'precision', 3):
            print(df.describe())
