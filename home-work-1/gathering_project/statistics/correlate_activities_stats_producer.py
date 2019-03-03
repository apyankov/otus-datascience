from statistics.stats_producer import StatsProducer


class ActivityStatsProducer(StatsProducer):
    question = """
    Вопрос N2
    Существует ли коррелиция между разными проявлениями активности пользователей?
    """

    fields = ['comments_count', 'likes_count', 'reposts_count', 'views_count']

    def print_stats(self, df):
        self.print_question(self.question)

        print(df.corr())
