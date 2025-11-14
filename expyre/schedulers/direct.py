import os
import json
import re

from ..subprocess import subprocess_run
from ..units import time_to_HMS, time_to_sec
from .. import util
import subprocess

from .base import Scheduler

class Direct(Scheduler):
    """
    Direct scheduler: executes jobs immediately on the remote machine
    using nohup and timeout, without any queuing system.
    """

    def __init__(self, host, remsh_cmd=None):
        super().__init__(host, remsh_cmd=remsh_cmd)
        self.cancel_command = ["kill"]  # compatible with base Scheduler

    def submit(self, id, remote_dir, partition=None, commands=None, max_time="1h",
                header=None, node_dict=None, no_default_header=False,
                script_exec="/bin/bash", pre_submit_cmds=None, verbose=False):
        """Submit a job on a remote machine

        Parameters
        ----------
        id: str
            unique job id (local)
        remote_dir: str
            remote directory where files have already been prepared and job will run
        partition: str
            partition (or queue or node type)
        commands: list(str)
            list of commands to run in script
        max_time: int
            time in seconds to run
        header: list(str)
            list of header directives, not including walltime specific directive
        node_dict: dict
            properties related to node selection.
            Fields: num_nodes, num_cores, num_cores_per_node, ppn, id, max_time, partition (and its synonum queue)
        no_default_header: bool, default False
            do not add normal header fields, only use what's passed in in "header"
        script_exec: str, default '/bin/bash'
            executable for first line of job script
        pre_submit_cmds: list(str), default []
            command to run in the remote process that does the submission before the actual submission,
            e.g. to fix the environment

        Returns
        -------
        str remote process id NOTE this is different to the otther classes which return job ids
        """
        #all node_dict stuff is probably unecessary
        node_dict = node_dict.copy()
        node_dict['id'] = id
        node_dict['max_time'] = time_to_HMS(max_time)
        node_dict['partition'] = partition
        node_dict['queue'] = partition
        header = header.copy()
        if not no_default_header:
            header.append('#SBATCH --job-name={id}')
            header.append('#SBATCH --time={max_time}')
            header.append('#SBATCH --output=job.{id}.stdout')
            header.append('#SBATCH --error=job.{id}.stderr')

        header.extend(json.loads(os.environ.get("EXPYRE_HEADER_EXTRA", "[]")))
        
        pre_commands = []
        # add "cd remote_dir" before any other command
        if remote_dir.startswith('/'):
            pre_commands.append(f'cd {remote_dir}')
        else:
            pre_commands.append(f'cd ${{HOME}}/{remote_dir}')
        
        #form the main script to be run
        script = '#!' + script_exec + '\n'
        script += '\n'.join([line.rstrip().format(**node_dict) for line in header]) + '\n'
        script += '\n' + '\n'.join([line.rstrip() for line in commands]) + '\n'

        #submit a job which write the main job script
        submit_args = Scheduler.unset_scheduler_env_vars("SLURM")
        submit_args += pre_submit_cmds + (['&&'] if len(pre_submit_cmds) > 0 else [])
        submit_args += ['cd', remote_dir, '&&', 'cat', '>', 'job.script.slurm',
                        '&&', "chmod" , "+x" ,'job.script.slurm']
        stdout, stderr = subprocess_run(self.host, args=submit_args, script=script, remsh_cmd=self.remsh_cmd, verbose=verbose)
        
        
        #2. submit a second job which actually executes the task
        timeout_s = f"{time_to_sec(max_time)}s"
        submit_args = ['cd', remote_dir, ';']
        submit_args += ['setsid', 'timeout', timeout_s, './job.script.slurm',  '>', 'job.log', '2>&1', '<', '/dev/null', '&' 'echo', '$!']        
        stdout, stderr = subprocess_run(self.host, args=submit_args, remsh_cmd=self.remsh_cmd, verbose=verbose)
        pid_str = stdout.strip().splitlines()[-1] if stdout.strip() else ""
        #NOTE against all odds this is working!
        
        return int(pid_str)
        


    def status(self, remote_ids, verbose=False):
        """
        Query remote job(s) by PID(s).
        Returns dict of {pid: 'running'|'done'|'timeout'}
        """
        if isinstance(remote_ids, int):
            remote_ids = [remote_ids]
        elif isinstance(remote_ids, str):
            remote_ids = [int(remote_ids)]
        

        statuses = {}
        for pid in remote_ids:
            #1. check if process running 
            submit_args = ["ps", "-p", str(pid)]
            stdout, stderr = subprocess_run(self.host, args=submit_args, remsh_cmd=self.remsh_cmd, verbose=verbose)
            if str(pid) in stdout:
                statuses[pid] = "running"
            else:
                statuses[pid] = "done"

        return statuses
   
