from base import PipelineOp, run_command

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
        'fp_fit': FPFit,
        }
