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
        """
        The object will be initialized with the parameters specified
        in the pipeline.in file.
        """
        self.data = params
    def status(self):
        """
        The .status function should assess whether the analysis has
        been run and whether that was successful, and return one of
        the following strings:

            'new', 'ok', 'fail'
        """
        return 'new'
    def run(self):
        """
        The .run function should attempt to create the pipeline
        product; it should treturn True or False depending on whether
        that was successful.
        """
        return False
    def subst(self, text):
        return substitute(self.data, text)

class PipelineScriptOp(PipelineOp):
    """
    A simple building block operation.  Assumes that the success or
    failure of the operation can be assessed based on the presence of
    certain files.  A typical parameter block might look like:
    {
    """
    def __init__(self, params):
        PipelineOp.__init__(self, params)
        self.ok_file = params.get('ok_file')
        self.ran_file = params.get('ran_file', self.ok_file)
        self.cmd = params.get('execute')
        self.places = params.get('make_place', [])

    def status(self):
        for f in [self.ok_file]:
            if len(glob.glob(self.subst(f))) > 0:
                return 'ok'
        was_run = len(glob.glob(self.subst(self.ran_file)))
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
            for f2 in glob.glob(self.subst(f)):
                print 'Removing', f2
                os.remove(f2)

    def reset_ok(self):
        for f in [self.ran_file, self.ok_file]:
            for f2 in glob.glob(self.subst(f)):
                print 'Removing', f2
                os.remove(f2)
        
