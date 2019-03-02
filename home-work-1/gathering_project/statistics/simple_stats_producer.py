from statistics.stats_producer import StatsProducer


class SimpleStatsProducer(StatsProducer):
    fields = df_1 = ['group_id', 'comments_count', 'likes_count', 'reposts_count', 'views_count']

    def print_stats(self, df):
        """
        Производим вычисления
        :param df: dataframe для обработки
        :return: список string для отображеиня статистики
        """
        self.print_basic_stats(df)
        self.print_top_active_groups(df)
        return

    def print_basic_stats(self, df):
        print('*** Basic info ***')
        df.info()
        print(df.describe())

        print('*** Correlation for fields ***')
        corr_fields = self.fields[1:]
        corr_df = df[corr_fields]
        print(corr_df.corr())

    def print_top_active_groups(self, df):
        """
        Вопрос N3:
        В каких сообществах сообщения вызывают наибольший интерес?
        """
        import pandas as pd
        import numpy as np
        import matplotlib
        from datetime import datetime
        from datetime import date

        df = df.copy()
        # создаем новый столбец
        df['interest_rate'] = df.apply(self.interest_func, axis=1)
        # удалим записи, где интерес = 0 - они нас не интересуют
        df = df[df['interest_rate'] > 0]

        # разделяем записи на 1: слабая активность, 2: адекватная активность, 3: подозрительно высокая активность
        # в notebook подобрали эти значение
        quantile_low = 0.85  # меньше этого значения - слабый интерес, выше - интерес есть
        quantile_high = 0.97  # выше этого значения - выброс. То есть, накрутка или что-либо еще.

        split_dfs = self.split_by_percentiles(df, 'interest_rate', quantile_low, quantile_high)
        target_df = split_dfs['target']

        print('статистика по сообщениям, которые вызвали хорошую активность:')
        print(target_df.describe())

        print('в этих сообществах встречается более всего публикаций, которые вызывают интерес у посетителей:')

    # введем величину, которую считаем "интерес к публикации"
    @staticmethod
    def interest_func(row):
        koef_comments = 1.5  # коэффициент, с которым вносим комментарий
        koef_likes = 1.0  # коэффициент, с которым вносим лайки
        koef_reposts = 2.0  # коэффициент, с которым вносим репосты

        return koef_comments * row['comments_count'] + koef_likes * row['likes_count'] + koef_reposts * row[
            'reposts_count']

    @staticmethod
    def split_by_percentiles(df, column_name, low, high):
        barrier_low = df[column_name].quantile(low)  # в виде числа
        barrier_high = df[column_name].quantile(high)  # в виде числа
        df_minor = df[df[column_name] < barrier_low].copy()
        df_tmp = df[df[column_name] >= barrier_low].copy()
        df_target = df_tmp[df_tmp[column_name] < barrier_high].copy()
        df_untrust = df_tmp[df_tmp[column_name] >= barrier_high].copy()

        return {
            'minor': df_minor,
            'target': df_target,
            'untrust': df_untrust
        }

    def reveal_groups(self, df):
        """
        найдем сообщества, в которых чаще всего встречаются сообщения из целевой группы
        :return: Series, index=group_id, value=кол-во записей
        """
        # теперь найдем сообщества, в которых чаще всего встречаются сообщения из целевой группы
        df_grouped = df.groupby('group_id').first() # создаем dataframe с уникальными group_id
        df_grouped['count'] = df['group_id'].value_counts() # проставляем кол-во, сколько раз встречается
        series_top = df_grouped['count'].sort_values(ascending=False)
        return series_top
