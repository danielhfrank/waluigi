from waluigi import WaluigiTask
from luigi import LocalTarget

from mrjob.examples.mr_word_freq_count import MRWordFreqCount

class DF(WaluigiTask):

    def input(self):
        return [LocalTarget('mobydick.txt')]

    def output(self):
        return LocalTarget('results')

if __name__ == '__main__':
    t = DF(job_cls=MRWordFreqCount)
    t.run()
