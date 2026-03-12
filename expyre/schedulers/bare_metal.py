import os
import json

from ..subprocess import subprocess_run
from ..units import time_to_HMS
from .. import util

from .base import Scheduler


class BareMetal(Scheduler):
    """Create BareMetal scheduler for running jobs directly on remote machines
    without any queuing system. Jobs run as background processes via SSH using
    ``nohup`` and are tracked by PID.

    The ``timeout`` command (from GNU coreutils) is used on the remote machine
    to enforce max_time limits when max_time is provided.

    Only one job runs at a time per submission (no queuing). Hold/release
    operations are not supported.

    The remote_id returned by submit() is a composite string ``PID::remote_dir``
    so that status() can locate the job's exit status file without needing
    external state.

    Parameters
    ----------
    host: str
        username and host for ssh/rsync username@machine.fqdn, or None for local
    remsh_cmd: str, default EXPYRE_RSH env var or 'ssh'
        remote shell command to use
    """
    def __init__(self, host, remsh_cmd=None):
        self.host = host
        self.hold_command = None
        self.release_command = None
        self.cancel_command = None
        self.remsh_cmd = util.remsh_cmd(remsh_cmd)


    @staticmethod
    def _parse_remote_id(remote_id):
        """Parse composite remote_id into PID and remote_dir.

        Parameters
        ----------
        remote_id: str
            composite ID in format ``PID::remote_dir``

        Returns
        -------
        pid: str
            process ID on remote machine
        remote_dir: str
            remote directory path for the job
        """
        parts = remote_id.split('::', 1)
        if len(parts) != 2:
            raise ValueError(f'Invalid bare_metal remote_id format: {remote_id}')
        return parts[0], parts[1]


    def submit(self, id, remote_dir, partition, commands, max_time, header, node_dict, no_default_header=False,
               script_exec="/bin/bash", pre_submit_cmds=[], verbose=False):
        """Submit a job on a remote machine by running it as a background process.

        Creates a job script and runs it with ``nohup`` in the background.
        The PID and remote_dir are encoded into the returned remote_id for later
        status checking. An EXIT trap in the script writes the process exit code
        to ``_expyre_exit_status`` for reliable completion detection.

        Parameters
        ----------
        id: str
            unique job id (local)
        remote_dir: str
            remote directory where files have already been prepared and job will run
        partition: str
            partition (kept for interface compatibility, not used for scheduling)
        commands: list(str)
            list of commands to run in script
        max_time: int
            time in seconds to run (enforced via timeout command if > 0)
        header: list(str)
            list of header lines to include in script
        node_dict: dict
            properties related to node selection.
            Fields: num_nodes, num_cores, num_cores_per_node, ppn, id, max_time, partition (and its synonym queue)
        no_default_header: bool, default False
            ignored for bare metal (no scheduler-specific headers to add)
        script_exec: str, default '/bin/bash'
            executable for first line of job script
        pre_submit_cmds: list(str), default []
            commands to run in the remote process before the actual job start,
            e.g. to fix the environment

        Returns
        -------
        str composite remote id ``PID::remote_dir``
        """
        node_dict = node_dict.copy()
        node_dict['id'] = id
        node_dict['max_time'] = time_to_HMS(max_time) if max_time is not None else '0:00:00'
        node_dict['partition'] = partition
        node_dict['queue'] = partition

        header = header.copy()
        # No default scheduler-specific headers for bare metal

        header.extend(json.loads(os.environ.get("EXPYRE_HEADER_EXTRA", "[]")))

        # set EXPYRE_NUM_CORES_PER_NODE - for bare metal, just use value from node_dict
        pre_commands = [
            f'export EXPYRE_NUM_CORES_PER_NODE={node_dict["num_cores_per_node"]}'
        ] + Scheduler.node_dict_env_var_commands(node_dict)
        pre_commands = [l.format(**node_dict) for l in pre_commands]

        # add "cd remote_dir" before any other command
        if remote_dir.startswith('/'):
            pre_commands.append(f'cd {remote_dir}')
        else:
            pre_commands.append(f'cd ${{HOME}}/{remote_dir}')

        commands = pre_commands + commands

        # Determine path prefix for the status file (must be absolute or $HOME-relative
        # so the trap writes to the correct location regardless of later cd commands)
        if remote_dir.startswith('/'):
            status_dir = remote_dir
        else:
            status_dir = f'$HOME/{remote_dir}'

        # Build job script
        script = '#!' + script_exec + '\n'
        script += '\n'.join([line.rstrip().format(**node_dict) for line in header]) + '\n'

        # Add EXIT trap to write process exit status to a file.
        # This fires on normal exit, signal-induced exit (SIGTERM from timeout), etc.
        # Uses an absolute path so it works even if user commands cd elsewhere.
        script += '\n'
        script += f'_EXPYRE_STATUS_DIR="{status_dir}"\n'
        script += '_expyre_bare_metal_cleanup() { echo $? > "$_EXPYRE_STATUS_DIR/_expyre_exit_status"; }\n'
        script += 'trap _expyre_bare_metal_cleanup EXIT\n'
        script += '\n'

        script += '\n'.join([line.rstrip() for line in commands]) + '\n'

        # Build submission command
        submit_args = pre_submit_cmds + (['&&'] if len(pre_submit_cmds) > 0 else [])

        # Write the script to a file via stdin, then run with nohup in background, echo PID
        submit_args += ['cd', remote_dir, '&&', 'cat', '>', 'job.script.bare_metal']

        if max_time is not None and max_time > 0:
            submit_args += ['&&', 'nohup', 'timeout', f'{int(max_time)}',
                            'bash', 'job.script.bare_metal',
                            '>', f'job.{id}.stdout', '2>', f'job.{id}.stderr', '&',
                            'echo', '$!']
        else:
            submit_args += ['&&', 'nohup', 'bash', 'job.script.bare_metal',
                            '>', f'job.{id}.stdout', '2>', f'job.{id}.stderr', '&',
                            'echo', '$!']

        stdout, stderr = subprocess_run(self.host, args=submit_args, script=script,
                                        remsh_cmd=self.remsh_cmd, verbose=verbose)

        # parse stdout for PID
        pid = None
        for line in stdout.splitlines():
            stripped = line.strip()
            if stripped.isdigit():
                pid = stripped
                break

        if pid is None:
            raise RuntimeError(f'Failed to get PID from bare metal job submission, stdout: {stdout}')

        # Return composite remote_id encoding both PID and remote_dir
        return f'{pid}::{remote_dir}'


    def status(self, remote_ids, verbose=False):
        """Determine status of remote jobs by checking exit status files and process liveness.

        Makes a single SSH call to check all jobs efficiently. For each job:

        1. If ``_expyre_exit_status`` file exists, reads it to determine done/failed/timeout
        2. Otherwise, checks if the PID is still running via ``kill -0``
        3. If the PID is gone and no exit status file exists, reports as failed (died)

        Parameters
        ----------
        remote_ids: str, list(str)
            list of composite remote ids (PID::remote_dir) to check

        Returns
        -------
        dict { str remote_id: str status},  status is one of :
                "queued", "held", "running",   "done", "failed", "timeout", "other"
            all remote ids passed in are guaranteed to be keys in dict
        """
        if isinstance(remote_ids, str):
            remote_ids = [remote_ids]

        if len(remote_ids) == 0:
            return {}

        # Build a single bash script to check all jobs in one SSH call
        check_lines = []
        for i, remote_id in enumerate(remote_ids):
            pid, remote_dir = self._parse_remote_id(remote_id)
            check_lines.extend([
                f'echo "EXPYRE_STATUS_BEGIN:{i}"',
                f'if [ -f "{remote_dir}/_expyre_exit_status" ]; then',
                f'    exit_code=$(cat "{remote_dir}/_expyre_exit_status" 2>/dev/null)',
                f'    echo "FINISHED:$exit_code"',
                f'elif kill -0 {pid} 2>/dev/null; then',
                f'    echo "RUNNING"',
                f'else',
                f'    echo "DIED"',
                f'fi',
            ])

        check_script = '\n'.join(check_lines) + '\n'

        stdout, stderr = subprocess_run(self.host, ['bash'],
            script=check_script,
            remsh_cmd=self.remsh_cmd, verbose=verbose)

        # Parse structured output
        out = {}
        current_idx = None
        for line in stdout.strip().splitlines():
            line = line.strip()
            if line.startswith('EXPYRE_STATUS_BEGIN:'):
                try:
                    current_idx = int(line.split(':', 1)[1])
                except (ValueError, IndexError):
                    current_idx = None
            elif current_idx is not None and 0 <= current_idx < len(remote_ids):
                remote_id = remote_ids[current_idx]
                if line.startswith('FINISHED:'):
                    exit_code_str = line.split(':', 1)[1].strip()
                    try:
                        exit_code = int(exit_code_str)
                    except ValueError:
                        exit_code = -1

                    if exit_code == 0:
                        out[remote_id] = 'done'
                    elif exit_code in (124, 137, 143):
                        # 124: timeout command exit code
                        # 137: SIGKILL (128+9)
                        # 143: SIGTERM (128+15)
                        out[remote_id] = 'timeout'
                    else:
                        out[remote_id] = 'failed'
                elif line == 'RUNNING':
                    out[remote_id] = 'running'
                elif line == 'DIED':
                    out[remote_id] = 'failed'
                else:
                    out[remote_id] = 'other'
                current_idx = None

        # IDs not found in output default to 'done' (consistent with other schedulers)
        for remote_id in remote_ids:
            if remote_id not in out:
                out[remote_id] = 'done'

        return out


    def hold(self, remote_ids, verbose=False):
        """Hold is not supported for bare metal scheduler.

        Bare metal runs jobs directly as background processes with no queuing,
        so there is no concept of holding a queued job.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError('Hold is not supported for bare metal scheduler - '
                                  'jobs run immediately as background processes')


    def release(self, remote_ids, verbose=False):
        """Release is not supported for bare metal scheduler.

        Bare metal runs jobs directly as background processes with no queuing,
        so there is no concept of releasing a held job.

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError('Release is not supported for bare metal scheduler - '
                                  'jobs run immediately as background processes')


    def cancel(self, remote_ids, verbose=False):
        """Cancel remote jobs by killing their processes.

        Sends SIGTERM to each job's process, which triggers the EXIT trap
        to write the exit status file before the process dies.

        Parameters
        ----------
        remote_ids: str, list(str)
            composite remote ids of jobs to cancel
        verbose: bool, default False
            verbose output
        """
        if isinstance(remote_ids, str):
            remote_ids = [remote_ids]

        pids = [self._parse_remote_id(rid)[0] for rid in remote_ids]
        subprocess_run(self.host, args=['kill'] + pids,
                       remsh_cmd=self.remsh_cmd, verbose=verbose)
