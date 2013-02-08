import itertools

from mrjob.job import MRJob
from luigi import Task, Parameter

from mrjob.examples.mr_word_freq_count import MRWordFreqCount

class WaluigiMRJobParameter(Parameter):
    
    def __init__(self, flag, *args, **kwargs):
        # flag is something like '-r' that can be passed as a cmd line option to mrjob
        assert isinstance(flag, str)
        self.flag = flag
        super(WaluigiMRJobParameter, self).__init__(*args, **kwargs)

class WaluigiTask(Task):
    '''
    Class that runs a MRJob from a Luigi workflow
    '''

    job_cls = Parameter()
    runner = WaluigiMRJobParameter('-r', default='local')

    def __init__(self, *args, **kwargs):
        print self.__dict__
        super(WaluigiTask, self).__init__(*args, **kwargs)
        

    def run(self):
        opts = self.make_mrjob_opts()
        print opts
        job = self.job_cls(args=opts)
        assert isinstance(job, MRJob)
        jobrunner = job.make_runner()
        jobrunner.run()

    @classmethod
    def get_mrjob_params_and_flags(cls):
        return [(param_name, param.flag) for param_name, param in cls.__dict__.items() 
            if isinstance(param, WaluigiMRJobParameter)]

    def make_mrjob_opts(self):
        prms_and_flags = self.get_mrjob_params_and_flags()
        return list(itertools.chain(*((flag, getattr(self, param_name)) for param_name, flag
            in prms_and_flags)))
        
if __name__ == '__main__':
    t = WaluigiTask(runner='df', job_cls=MRWordFreqCount)
    t.run()
