from statistics.stats_producer import StatsProducer
import pandas as pd


class MessageTextLenStatsProducer(StatsProducer):
    question = """
    Вопрос N4
    Существует ли корреляция между длиной текста и какой-либо активностью пользователей применительно к message?
    """

    fields = ['text_len', 'comments_count', 'likes_count', 'reposts_count', 'views_count']

    def print_stats(self, df):
        self.print_question(self.question)

        with pd.option_context('display.max_columns', 10):
            print(df.corr())
