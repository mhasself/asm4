"""
asm4?  Yes.
"""
import subprocess as sp
import os

def run_command(cmd):
    if isinstance(cmd, basestring):
        cmd = cmd.split()
    p = sp.Popen(cmd)
    p.communicate()
    return p.returncode

def substitute(kwargs, s, max_depth=10):
    output = s.format(**kwargs)
    if max_depth > 0 and '{' in output:
        return substitute(kwargs, output, max_depth-1)
    return output

class PipelineOp(object):
    def __init__(self, params):
        self.data = params
    def status(self):
        return 'new'
    def run(self):
        return False
    def subst(self, text):
        return substitute(self.data, text)

class PipelineScriptOp(PipelineOp):
    def __init__(self, params):
        PipelineOp.__init__(self, params)
        self.ran_file = params['ran_file']
        self.ok_file = params['ok_file']
        self.cmd = params['execute']
        self.places = params.get('make_place', [])

    def status(self):
        for f in [self.ok_file]:
            if os.path.exists(self.subst(f)):
                return 'ok'
        was_run = os.path.exists(self.ran_file)
        if was_run:
            return 'fail'
        return 'new'

    def run(self):
        for p in self.places:
            if not os.path.exists(p):
                os.makedirs(p)
        cmd = self.subst(self.cmd)
        return (run_command(cmd) == 0)

    def reset_failed(self):
        if os.path.exists(self.run_file):
            os.remove(self.run_file)

    def reset_ok(self):
        for f in self.map_files:
            if os.path.exists(f):
                os.remove(self.map_file)
        
