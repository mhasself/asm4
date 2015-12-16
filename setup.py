from distutils.core import setup, Extension

VERSION = '0.1'

import glob

## Everything in bin is a script
scripts = [x for x in glob.glob('bin/*') if x[-1] != '~']

setup (name = 'asm4',
       version = VERSION,
       description = 'Generic Pipeline Paradigm 4',
       package_dir = {'asm4': 'python'},
       scripts = scripts,
       packages = ['asm4'])
