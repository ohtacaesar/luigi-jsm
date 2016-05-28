# coding:utf-8

import datetime
import os

import jsm
import luigi
import pandas as pd

q = jsm.Quotes()


class LoadPricesWorkflow(luigi.WrapperTask):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        task = ExtractBrandsTask(date=self.date)
        yield task

        df = pd.read_csv(task.output().path, index_col='code')
        codes = list(df.head().index)
        for code in codes:
            yield ExtractPriceTask(date=self.date, code=code)


class MakeDateDirTask(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget('data/%s' % str(self.date))

    def run(self):
        os.makedirs(self.output().path)


class ExtractBrandsTask(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return MakeDateDirTask(date=self.date)

    def output(self):
        return luigi.LocalTarget('data/%s/brands.csv' % str(self.date))

    def run(self):
        # '5250' # 情報・通信 
        brands = q.get_brand('5250')
        df = pd.DataFrame([[b.ccode, b.market, b.name, b.info] for b in brands])
        df.columns = ['code', 'market', 'name', 'info']
        df = df.set_index('code')
        df.to_csv(self.output().path, encoding='utf-8')


class ExtractPriceTask(luigi.Task):
    date = luigi.DateParameter()
    code = luigi.IntParameter()

    def requires(self):
        return MakeDateDirTask(date=self.date)

    def output(self):
        return luigi.LocalTarget('data/%s/%s.csv' % (str(self.date), self.code))

    def run(self):
        prices = q.get_historical_prices(self.code)
        df = pd.DataFrame([[p.date, p.open, p.high, p.low, p.close, p.volume, p._adj_close] for p in prices])
        df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'adj_close']
        df = df.set_index('date')
        df.to_csv(self.output().path, encoding='utf-8')
