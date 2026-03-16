import os
import sys
import json
import logging

from ..subprocess import subprocess_run
from ..units import time_to_HMS
from .. import util

from .base import Scheduler

logger = logging.getLogger(__name__)


class BareMetal(Scheduler):
    """Create BareMetal scheduler for running jobs directly on remote machines
    without any queuing system. Jobs run as background processes via SSH using
    ``nohup`` and are tracked by PID.

    The ``timeout`` command (from GNU coreutils) is used on the remote machine
    to enforce max_time limits when max_time is provided.

    Only one job runs at a time per submission (no queuing). Hold/release
    operations are not supported.

    The remote_id returned by submit() is a composite string
    ``bare_metal::PID::remote_dir`` so that status() can locate the job's exit
    status file without needing external state, and the prefix makes it
    unambiguously identifiable as a bare_metal ID.

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
            composite ID in format ``bare_metal::PID::remote_dir``

        Returns
        -------
        pid: str
            process ID on remote machine
        remote_dir: str
            remote directory path for the job
        """
        parts = remote_id.split('::', 2)
        if len(parts) != 3 or parts[0] != 'bare_metal':
            raise ValueError(f'Invalid bare_metal remote_id format: {remote_id}')
        return parts[1], parts[2]


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
        str composite remote id ``bare_metal::PID::remote_dir``
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

        # Step 1: Write the job script to the remote machine via stdin.
        # This MUST be a separate SSH call because the '&' operator used to
        # background the job in step 2 causes bash (in non-interactive mode)
        # to redirect stdin from /dev/null for the entire backgrounded command
        # group. If cat were in the same command, it would read nothing and
        # write an empty script file.
        write_args = pre_submit_cmds + (['&&'] if len(pre_submit_cmds) > 0 else [])
        write_args += ['cd', remote_dir, '&&', 'cat', '>', 'job.script.bare_metal']

        subprocess_run(self.host, args=write_args, script=script,
                       remsh_cmd=self.remsh_cmd, verbose=verbose)

        logger.info(f'bare_metal submit [{self.host}]: wrote job script to {remote_dir}/job.script.bare_metal '
                    f'({len(script)} bytes)')

        # Step 2: Start the job as a background process and capture its PID.
        # No stdin is piped, so backgrounding with & is safe here.
        if max_time is not None and max_time > 0:
            run_args = ['cd', remote_dir, '&&',
                        'nohup', 'timeout', f'{int(max_time)}',
                        'bash', 'job.script.bare_metal',
                        '>', f'job.{id}.stdout', '2>', f'job.{id}.stderr', '&',
                        'echo', '$!']
        else:
            run_args = ['cd', remote_dir, '&&',
                        'nohup', 'bash', 'job.script.bare_metal',
                        '>', f'job.{id}.stdout', '2>', f'job.{id}.stderr', '&',
                        'echo', '$!']

        stdout, stderr = subprocess_run(self.host, args=run_args,
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

        composite_id = f'bare_metal::{pid}::{remote_dir}'
        logger.info(f'bare_metal submit [{self.host}]: job started with PID {pid}, '
                    f'remote_id={composite_id}')

        # Return composite remote_id encoding both PID and remote_dir
        return composite_id


    def status(self, remote_ids, verbose=False):
        """Determine status of remote jobs by checking exit status files and process liveness.

        Makes a single SSH call to check all jobs efficiently. For each job:

        1. If ``_expyre_exit_status`` file exists, reads it to determine done/failed/timeout
        2. Otherwise, checks if the PID is still running via ``kill -0``
        3. If the PID is gone and no exit status file exists, reports as failed (died)

        Parameters
        ----------
        remote_ids: str, list(str)
            list of composite remote ids (bare_metal::PID::remote_dir) to check

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

        out = {}

        # Pre-filter remote_ids: only those with the PID::remote_dir format are valid
        # bare_metal IDs. Others (e.g. stale slurm/pbs numeric IDs left in the DB after
        # switching scheduler) are treated as 'done' so they don't block result syncing.
        valid_entries = []  # list of (remote_id, pid, remote_dir)
        for remote_id in remote_ids:
            try:
                pid, remote_dir = self._parse_remote_id(remote_id)
                valid_entries.append((remote_id, pid, remote_dir))
            except ValueError:
                logger.warning(f'bare_metal status [{self.host}]: skipping invalid remote_id '
                               f'{remote_id!r} (likely from a previous scheduler), treating as done')
                out[remote_id] = 'done'

        if not valid_entries:
            # All IDs were invalid/stale, nothing to check remotely
            return out

        # Build a single bash script to check all valid jobs in one SSH call
        check_lines = []
        for seq, (remote_id, pid, remote_dir) in enumerate(valid_entries):
            check_lines.extend([
                f'echo "EXPYRE_STATUS_BEGIN:{seq}"',
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
        current_idx = None
        for line in stdout.strip().splitlines():
            line = line.strip()
            if line.startswith('EXPYRE_STATUS_BEGIN:'):
                try:
                    current_idx = int(line.split(':', 1)[1])
                except (ValueError, IndexError):
                    current_idx = None
            elif current_idx is not None and 0 <= current_idx < len(valid_entries):
                remote_id = valid_entries[current_idx][0]
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

        logger.info(f'bare_metal status [{self.host}]: {out}')

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

        Gracefully skips remote_ids that don't match the bare_metal format
        (e.g. stale slurm/pbs IDs left in the database).

        Parameters
        ----------
        remote_ids: str, list(str)
            composite remote ids of jobs to cancel
        verbose: bool, default False
            verbose output
        """
        if isinstance(remote_ids, str):
            remote_ids = [remote_ids]

        pids = []
        for rid in remote_ids:
            try:
                pid, _ = self._parse_remote_id(rid)
                pids.append(pid)
            except ValueError:
                logger.warning(f'bare_metal cancel [{self.host}]: skipping invalid remote_id '
                               f'{rid!r} (likely from a previous scheduler)')

        if pids:
            subprocess_run(self.host, args=['kill'] + pids,
                           remsh_cmd=self.remsh_cmd, verbose=verbose)
