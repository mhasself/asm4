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
        cmd = self.subst('planetMap {params_file} {tod_id} '
                         '-o {output_prefix}')
        return (run_command(cmd) == 0)

    def reset_failed(self):
        if os.path.exists(self.run_file):
            os.remove(self.run_file)

    def reset_ok(self):
        for f in self.map_files + [self.run_file]:
            f = self.subst(f)
            if os.path.exists(f):
                os.remove(f)
        
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
