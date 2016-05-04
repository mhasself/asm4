from base import PipelineOp, PipelineScriptOp, run_command

import moby2
import os

class TODExists(PipelineOp):
    def status(self):
        fn = moby2.scripting.get_filebase().get_full_path(self.data['tod_name'])
        if fn is None:
            return 'fail'
        return 'ok'

class TODCuts(PipelineOp):
    def __init__(self, params):
        PipelineOp.__init__(self, params)
        self.cuts_path = '{cuts_depot}/TODCuts/{cuts_tag}/{first_five}/{tod_name}.cuts'
        self.run_file = self.subst('{output_prefix}cuts.dict')

    def status(self):
        if os.path.exists(self.subst(self.cuts_path)):
            return 'ok'
        was_run = os.path.exists(self.run_file)
        if was_run:
            return 'fail'
        return 'new'

    def run(self):
        cmd = self.subst('planetCuts {params_file} {tod_name} '
                         '-o {output_prefix}')
        return (run_command(cmd) == 0)

    def reset_failed(self):
        if os.path.exists(self.run_file):
            os.remove(self.run_file)

    def reset_ok(self):
        if os.path.exists(self.cuts_path):
            os.remove(self.cuts_path)
        
class TODMap(PipelineOp):
    def __init__(self, params):
        PipelineOp.__init__(self, params)
        self.map_files = ['{output_prefix}'+x for x in 
                          ['source.fits', 'source_I.fits',
                           'source_split00.fits', 'source_I_split00.fits']]
        self.run_file = self.subst('{output_prefix}planetMap.par')

    def status(self):
        for f in self.map_files:
            if os.path.exists(self.subst(f)):
                return 'ok'
        was_run = os.path.exists(self.run_file)
        if was_run:
            return 'fail'
        return 'new'

    def run(self):
        cmd = self.subst('planetMap {params_file} {tod_name} '
                         '-o {output_prefix}')
        return (run_command(cmd) == 0)

    def reset_failed(self):
        if os.path.exists(self.run_file):
            os.remove(self.run_file)

    def reset_ok(self):
        for f in self.map_files:
            if os.path.exists(f):
                os.remove(self.map_file)
        
class FPFit(PipelineOp):
    def status(self):
        if os.path.exists(self.subst(self.data['output_file'])):
            return 'ok'
        return 'new'

    def run(self):
        n_mpi = self.data.get('n_mpi', 4)
        cmd_prefix = 'mpirun -n %i ' % n_mpi
        cmd = cmd_prefix + self.subst('fp_fit {params_file} {tod_name}')
        return (run_command(cmd) == 0)

    def reset_ok(self):
        ofile = self.subst(self.data['output_file'])
        if os.path.exists(ofile):
            os.remove(ofile)
        
def get_modules():
    return {
        'tod_exists': TODExists,
        'planet_cuts': TODCuts,
        'planet_map': TODMap,
        'fp_fit': FPFit,
        'script': PipelineScriptOp,
        }
