import datetime
import os
import subprocess
import time as ttime
import uuid

from .utils import find_executable


def run_command(exec_name, inputs=None, *args, **kwargs):
    """
    Run a command with the specified executable.

    Parameters
    ----------
    exec_name : str
        the name of the executable
    inputs : list, optional
        input parameters to pass to the executable

    Returns
    -------
    st : subprocess.CompletedProcess
        The return value from run(), representing a process that has finished.
    """
    if inputs is None:
        inputs = []

    exec_path = find_executable(exec_name)
    cmd = [exec_path] + inputs

    st = subprocess.run(cmd, *args, **kwargs)

    return st


def run_with_dask(client, inputs, cwd,
                  # exec_name, input_file, cwd,
                  # prefix='test', symmetry='P1', mode='FAST', n_repeats=1,
                  # wait=False):
                  ):
    """
    Run parallel jobs with Dask.

    Parameters
    ----------
    client: dask.distributed.Client
        an instance of the Dask client
    inputs: list
        a list of dictionaries with the input parameters, i.e. the key will be
        the executable name, and the values will be lists of parameters
    """
    """
    # We have to make sure we are working with the absolute paths, as the
    # relative paths may be different in the eyes of a client and the Dask
    # SLURM scheduler.
    if not os.path.isabs(input_file):
        input_file = os.path.abspath(input_file)

    # Check if the input file exists, is actually a file, not a directory, and
    # has correct read permissions.
    try:
        with open(input_file, 'r'):
            ...
    except Exception as e:
        raise e
    """

    if not os.path.exists(cwd) and not os.path.isdir(cwd):
        try:
            os.makedirs(cwd, mode=0o777, exists_ok=True)
        except Exception:
            raise RuntimeError(f'The directory "{cwd}" was not created.')
    """
    # [
        {'exec': 'dammif',
         'exec_inputs': {'symmetry': 'P1', 'mode': 'FAST'},
         'n_repeats': 20,
         'prefix': 'test',
         'format': '02d',
         'input_file': '/nsls2/xf16id1/experiments/2019-1/301525/303773/mut3_20mgml_230-249s.out'},
        {'exec': 'damaver'
         'exec_inputs': {'automatic': None},
        ]
    """
    for elem in inputs:
        # dammif
        if elem['exec'] == 'dammif':
            futures = []
            futures_dict = {}
            for i in range(elem['n_repeats']):
                key = _generate_unique_key()
                exec_inputs = _construct_inputs(elem['exec_inputs'])

                formatted_prefix = f'{elem["prefix"]}{i:{elem["format"]}}'
                pdb_file = os.path.join(cwd, f'{formatted_prefix}-1.pdb')

                future = client.submit(run_command,
                                       elem['exec'],
                                       inputs=[elem['input_file'],
                                               f'--prefix={formatted_prefix}',
                                               *exec_inputs],
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT,
                                       shell=False, check=True,
                                       cwd=cwd, key=key)
                futures.append(future)
                futures_dict[key] = {'future': future,
                                     'formatted_prefix': formatted_prefix,
                                     'pdb_file': pdb_file}
            futures = client.gather(futures)

        # damaver
        elif elem['exec'] == 'damaver':
            key = _generate_unique_key()
            exec_inputs = _construct_inputs(elem['exec_inputs'])

            pdb_files = [v['pdb_file'] for k, v in futures_dict.items()]

            future = client.submit(run_command,
                                   elem['exec'],
                                   inputs=[*exec_inputs,
                                           *pdb_files],
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT,
                                   shell=False, check=True,
                                   cwd=cwd, key=key)

    return futures_dict, future


def _construct_inputs(inputs):
    return [f'--{k}' if v is None else f'--{k}={v}' for k, v in inputs.items()]


def _generate_unique_key():
    dt = datetime.datetime.fromtimestamp(ttime.time()).isoformat()
    uid = str(uuid.uuid4())[:8]
    key = f'{dt}-{uid}'
    return key
