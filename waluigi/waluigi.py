from itertools import chain
from operator import attrgetter

from luigi import Task

MRJOB_FLAGS = set(['-r', '-c', '--no-output', '--jobconf'])

def to_opts(flag, value):
    if flag not in MRJOB_FLAGS:
        return []
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

    job_cls = NotImplemented

    def mrjob_opts(self):
        return {}

    def __init__(self, *args, **kwargs):
        self.opts = self.make_mrjob_opts(*args, **kwargs)

    def run(self):
        job = self.job_cls(args=self.opts)
        jobrunner = job.make_runner()
        jobrunner.run()

    def make_mrjob_opts(self, *args, **kwargs):
        base_opts = self.mrjob_opts()
        base_opts.update(kwargs)
        opts_list = list(chain(*[to_opts(k,v) for k,v in base_opts]))
        # now add special params: input and ouput
        # yikes, existence of path field not really enforced
        input_files = map(attrgetter('path'), self.input())
        output_dir = self.output().path 
        assert input_files and output_dir, "Must define output and dependencies or direct input"
        return opts_list + input_files + ['-o', output_dir]
