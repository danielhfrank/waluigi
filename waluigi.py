from itertools import chain
from operator import attrgetter

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
    conf_path = WaluigiMRJobParameter('-c', default='')

    def __init__(self, *args, **kwargs):
        super(WaluigiTask, self).__init__(*args, **kwargs)
        jobconf = (kwargs.get('jobconf', {}))
        self.opts = self.make_mrjob_opts() + \
            list(chain(*[('--jobconf', '%s=%s' % (k,v)) for k,v in jobconf.items()]))
        # print self.opts


    def run(self):
        job = self.job_cls(args=self.opts)
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
        return list(chain(*(get_from(cl) for cl in cls.__mro__)))

    def make_mrjob_opts(self):
        prms_and_flags = self.get_mrjob_params_and_flags()
        base_params = list(chain(*((flag, getattr(self, param_name)) for param_name, flag
            in prms_and_flags if getattr(self, param_name))))
        # now add special params: input and ouput
        # yikes, existence of path field not really enforced
        input_files = map(attrgetter('path'), self.input())
        output_dir = self.output().path 
        assert input_files and output_dir, "Must define output and dependencies or direct input"
        return base_params + ['-o', output_dir] + input_files
