"""
asm4?  Yes.
"""
import subprocess as sp
import os, glob

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
        self.ok_file = params['ok_file']
        self.ran_file = params.get('ran_file', self.ok_file)
        self.cmd = params['execute']
        self.places = params.get('make_place', [])

    def status(self):
        for f in [self.ok_file]:
            if len(glob.glob(self.subst(f))) > 0:
                return 'ok'
        was_run = len(glob.glob(self.ran_file))
        if was_run:
            return 'fail'
        return 'new'

    def run(self):
        for p in self.places:
            p = self.subst(p)
            if not os.path.exists(p):
                os.makedirs(p)
        cmd = self.subst(self.cmd)
        return (run_command(cmd) == 0)

    def reset_failed(self):
        for f in [self.ran_file]:
            f = self.subst(f)
            if os.path.exists(f):
                os.remove(f)

    def reset_ok(self):
        for f in [self.ran_file, self.ok_file]:
            f = self.subst(f)
            if os.path.exists(f):
                os.remove(f)
        
