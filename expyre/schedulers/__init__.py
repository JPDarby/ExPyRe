"""Top level interface to all the implemented schedulers in expyre.schedulers_impl
"""
from .slurm import Slurm
from .pbs import PBS
from .local import Local
from .sge import SGE
from .direct import Direct

schedulers = {"slurm": Slurm, 'pbs': PBS, 'local': Local, 'sge': SGE, "direct": Direct}
