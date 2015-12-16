"""
asm4?  Yes.
"""
import subprocess as sp

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
