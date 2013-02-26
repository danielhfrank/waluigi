from itertools import chain
from operator import attrgetter

from luigi import Task

def to_opts(flag, value):
    if isinstance(value, bool):
        return [flag] if value else []
    elif isinstance(value, dict):
        # this is intended to be used for jobconf option
        return list(chain(*[(flag, '%s=%s' % (k,v)) for k,v in value.items()]))
    else:
        assert isinstance(value, str)
        return [flag, value]

class WaluigiTask(Task):
    '''
    Class that runs a MRJob from a Luigi workflow
    '''

    def job_cls(self):
        '''
        Override this to return the right mrjob class (not instance)
        '''
        raise NotImplementedError

    def mrjob_opts(self):
        return {}

    def __init__(self, *args, **kwargs):
        super(WaluigiTask, self).__init__(*args, **kwargs)
        self.opts = self.make_mrjob_opts(*args, **kwargs)

    def run(self):
        job = self.job_cls()(args=self.opts)
        jobrunner = job.make_runner()
        jobrunner.run()

    def make_mrjob_opts(self, *args, **kwargs):
        base_opts = self.mrjob_opts()
        base_opts.update((k,v) for k,v in kwargs.iteritems() if k.startswith('-'))
        opts_list = list(chain(*[to_opts(k,v) for k,v in base_opts.iteritems()]))
        # now add special params: input and ouput
        # yikes, existence of path field not really enforced
        input_files = map(attrgetter('path'), self.input())
        output_dir = self.output().path 
        assert input_files and output_dir, "Must define output and dependencies or direct input"
        return opts_list + input_files + ['-o', output_dir]
