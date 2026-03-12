"""Top level interface to all the implemented schedulers in expyre.schedulers_impl
"""
from .slurm import Slurm
from .pbs import PBS
from .local import Local
from .sge import SGE
from .bare_metal import BareMetal

schedulers = {"slurm": Slurm, 'pbs': PBS, 'local': Local, 'sge': SGE, 'bare_metal': BareMetal}
