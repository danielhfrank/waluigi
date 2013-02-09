import itertools
from operator import attrgetter

from mrjob.job import MRJob
from luigi import Task, Parameter


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
    runner = WaluigiMRJobParameter('-r', default='') 

    def run(self):
        opts = self.make_mrjob_opts()
        print opts
        job = self.job_cls(args=opts)
        assert isinstance(job, MRJob)
        jobrunner = job.make_runner()
        jobrunner.run()

    @classmethod
    def get_mrjob_params_and_flags(cls):
        '''
        Gets the WaluigiJobParameters from this and all parent classes
        '''
        def get_from(clazz):
            return [(param_name, param.flag) for param_name, param in clazz.__dict__.items() 
                if isinstance(param, WaluigiMRJobParameter)]
        return list(itertools.chain(*(get_from(cl) for cl in cls.__mro__)))

    def make_mrjob_opts(self):
        prms_and_flags = self.get_mrjob_params_and_flags()
        base_params = list(itertools.chain(*((flag, getattr(self, param_name)) for param_name, flag
            in prms_and_flags if getattr(self, param_name))))
        # now add special params: input and ouput
        # yikes, existence of path field not really enforced
        input_files = map(attrgetter('path'), self.input())
        output_dir = self.output().path 
        assert input_files and output_dir, "Must define output and dependencies or direct input"
        return base_params + ['-o', output_dir] + input_files
