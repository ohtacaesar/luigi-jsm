# coding:utf-8

import jsm
import luigi
import pandas as pd
import os

q = jsm.Quotes()


class ExtractBrandTask(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget('data/%s/brands.csv' % str(self.date))

    def run(self):
        # ディレクトリ作成
        os.makedirs(os.path.dirname(self.output().path))

        # '5250' # 情報・通信 
        brands = q.get_brand('5250')
        df = pd.DataFrame([[b.ccode, b.market, b.name, b.info] for b in brands])
        df.to_csv(self.output().path, index=False, header=False, encoding='utf-8')
