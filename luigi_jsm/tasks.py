# coding:utf-8

import jsm
import luigi
import pandas as pd

q = jsm.Quotes()


class ExtractBrandTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget('data/brands.csv')

    def run(self):
        # '5250' # 情報・通信 
        brands = q.get_brand('5250')
        df = pd.DataFrame([[b.ccode, b.market, b.name, b.info] for b in brands])
        df.to_csv(self.output().path, index=False, header=False, encoding='utf-8')
