from waluigi import WaluigiTask
from luigi import LocalTarget

from mrjob.examples.mr_word_freq_count import MRWordFreqCount

class DF(WaluigiTask):

    # def job_cls(self):
    #     return MRWordFreqCount

    job_cls = MRWordFreqCount

    def mrjob_opts(self):
        return {
            '--no-output': True,
            '--jobconf': {'mapred.job.name':'DF'}
        }

    def input(self):
        return [LocalTarget('mobydick.txt')]

    def output(self):
        return LocalTarget('results')

if __name__ == '__main__':
    t = DF()
    t.run()
