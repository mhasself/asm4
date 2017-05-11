from base import PipelineOp, PipelineScriptOp, run_command

import moby2
import os

        
def get_modules():
    """
    Returns a dict that associates a PipelineOp subclass with each
    eligible pipeline op 'type'.
    """
    return {
        'tod_exists': TODExists,
        'planet_cuts': TODCuts,
        'planet_map': TODMap,
        'fp_fit': FPFit,
        'script': PipelineScriptOp,
        }

class TODExists(PipelineOp):
    def status(self):
        fn = moby2.scripting.get_filebase().get_full_path(self.data['tod_id'])
        if fn is None:
            return 'fail'
        return 'ok'

class TODCuts(PipelineScriptOp):
    def __init__(self, params):
        params['execute_output_prefix'] = ('output_prefix' in params)
        if 'cuts_params' in params:
            # Try to decode the cuts config file...
            cuts_cfg = moby2.util.MobyDict.from_file(params['cuts_params'])
            # By rights any output prefix specified by the user should
            # override the cuts config output prefix... right now it
            # prepends, but I define that to be a bug.
            if not 'output_prefix' in params:
                params['output_prefix'] = cuts_cfg.get_deep(('output', 'prefix'))
            if not 'cuts_depot' in params:
                params['cuts_depot'] = cuts_cfg.get_deep(('output', 'depot'))
            if not 'cuts_tag' in params:
                params['cuts_tag'] = cuts_cfg.get_deep(('output', 'cuts_tag'))
        if not 'ran_file' in params:
            params['ran_file'] = '{output_prefix}cuts.dict'
        if not 'ok_file' in params:
            params['ok_file'] = '{cuts_depot}/TODCuts/{cuts_tag}/{first_five}/{tod_id}.cuts'
        # Surely the superclass can handle it from here.
        super(TODCuts, self).__init__(params)

    def run(self):
        cmd = 'planetCuts {cuts_params} {tod_id}'
        if self.data.get('execute_output_prefix'):
            cmd += ' -o {output_prefix}'
        cmd = self.subst(cmd)
        return (run_command(cmd) == 0)
        
class TODMap(PipelineScriptOp):
    def __init__(self, params):
        params['execute_output_prefix'] = ('output_prefix' in params)
        if 'params_file' in params:
            # Try to decode the cuts config file...
            map_cfg = moby2.util.MobyDict.from_file(params['params_file'])
            # By rights any output prefix specified by the user should
            # override the cuts config output prefix... right now it
            # prepends, but I define that to be a bug.
            if not 'output_prefix' in params:
                params['output_prefix'] = map_cfg.get_deep(('output', 'prefix'))
        params['ran_file'] = '{output_prefix}planetMap.par'
        params['ok_file'] = '{output_prefix}source_*.fits'
        super(TODMap, self).__init__(params)

    def run(self):
        cmd = 'planetMap {params_file} {tod_id}'
        if self.data.get('execute_output_prefix'):
            cmd += ' -o {output_prefix}'
        cmd = self.subst(cmd)
        return (run_command(cmd) == 0)

class FPFit(PipelineOp):
    def status(self):
        if os.path.exists(self.subst(self.data['output_file'])):
            return 'ok'
        return 'new'

    def run(self):
        cmd_prefix = ''
        n_mpi = self.data.get('n_mpi')
        if n_mpi is not None:
            cmd_prefix = 'mpirun -n %i ' % n_mpi
        cmd = cmd_prefix + self.subst('fp_fit {params_file} {tod_id}')
        return (run_command(cmd) == 0)

    def reset_ok(self):
        ofile = self.subst(self.data['output_file'])
        if os.path.exists(ofile):
            os.remove(ofile)
