import logging
import pathlib
import sys
from datetime import datetime

import luigi
import pytz

JST = pytz.timezone('Asia/Tokyo')

logger = logging.getLogger('luigi-interface')


class ForcibleTask(luigi.Task):
  force = luigi.BoolParameter(default=False)

  def complete(self):
    if self.force and not hasattr(self, '__ran'):
      setattr(self, '__ran', True)
      return False

    return super().complete()


class A(ForcibleTask):
  def output(self):
    return luigi.LocalTarget('/tmp/a')

  def run(self):
    path = pathlib.Path(self.output().path)
    with path.open('w') as f:
      f.write(datetime.now(JST).isoformat())
    logger.info('path.is_file() == {}'.format(path.is_file()))


class B(ForcibleTask):

  def requires(self):
    return A(force=self.force)

  def output(self):
    return luigi.LocalTarget('/tmp/b')

  def run(self):
    path = pathlib.Path(self.output().path)
    with path.open('w') as f:
      f.write(datetime.now(JST).isoformat())
    logger.info('path.is_file() == {}'.format(path.is_file()))


class C(ForcibleTask):

  def requires(self):
    return A(force=self.force)

  def output(self):
    return luigi.LocalTarget('/tmp/c')

  def run(self):
    path = pathlib.Path(self.output().path)
    with path.open('w') as f:
      f.write(datetime.now(JST).isoformat())
    logger.info('path.is_file() == {}'.format(path.is_file()))


class D(ForcibleTask):

  def requires(self):
    return [
      B(force=self.force),
      C(force=self.force),
    ]

  def output(self):
    return luigi.LocalTarget('/tmp/d')

  def run(self):
    path = pathlib.Path(self.output().path)
    with path.open('w') as f:
      f.write(datetime.now(JST).isoformat())
    logger.info('path.is_file() == {}'.format(path.is_file()))


if __name__ == '__main__':
  force = len(sys.argv) > 1
  logger.info('force == {}'.format(force))

  luigi.build([D(force=force)])
