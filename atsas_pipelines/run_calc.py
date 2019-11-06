import subprocess

from dask.distributed import Client

from .utils import find_executable


def run_calc(exec_name, inputs=None, *args, **kwargs):
    """
    Run an ATSAS simulation.

    Parameters
    ----------
    exec_name : str
        the name of the executable
    inputs : list
        input parameters to pass to the executable

    $ dammif --help
    Usage: dammif [OPTIONS] <GNOMFILE>

    rapid ab-initio shape determination in small-angle scattering

    Known Arguments:
    GNOMFILE                   GNOM output file with the data to fit

    Known Options:
    -h, --help                 Print usage information and exit
    -v, --version              Print version information and exit
    -q, --quiet                Reduce verbosity level
      --seed=<INT>           Set the seed for the random number generator
    -c, --chained              enable building of pseudo-chains in PDB output
    -u, --unit=<UNIT>          ANGSTROM, NANOMETRE or UNKNOWN (default: unknown)
    -p, --prefix=<PREFIX>      the PREFIX to prepend to any output filename (default: dammif)
    -a, --anisometry=<O|P>     Particle anisometry (Oblate/Prolate)
    -s, --symmetry=<PXY>       Particle symmetry
    -m, --mode=<MODE>          one of: FAST, SLOW, INTERACTIVE (default: interactive)
      --omit-solvent         omit output of solvent in PREFIX-0.pdb
      --constant=<VALUE>     constant to subtract, 0 to disable constant subtraction (automatic if undefined)
      --max-bead-count=<VALUEmaximum number of beads in search space (unlimited if undefined)

    Mandatory arguments to long options are mandatory for short options too.

    Report bugs to <atsas@embl-hamburg.de>.
    """
    if inputs is None:
        inputs = []

    exec_path = find_executable(exec_name)
    cmd = [exec_path] + inputs

    st = subprocess.run(cmd, *args, **kwargs)

    return st


def local_dask_client(n_workers=1):
    client = Client(threads_per_worker=n_workers,
                    n_workers=n_workers)
    return client


def slurm_dask_client(n_workers=1, queue=None, memory=None):
    from dask_jobqueue import SLURMCluster

    if queue is None:
        queue = 'lix-atsas'
    if memory is None:
        memory = '4GB'

    cluster = SLURMCluster(queue=queue, cores=n_workers, memory=memory)
    cluster.scale(jobs=n_workers)
    client = Client(cluster)
    return client


def run_with_dask(exec_name,
                  input_file, prefix='test', symmetry='P1', mode='FAST',
                  n_repeats=1, dask_client_type='local', n_workers=1,
                  queue=None, memory=None):

    supported_dask_clients = ('local', 'slurm')
    assert dask_client_type in supported_dask_clients, \
        (f'The specified dask client type "{dask_client_type}" '
         f'is not in the supported clients list: {supported_dask_clients}')

    if dask_client_type == 'local':
        client = local_dask_client(n_workers=n_workers)
    elif dask_client_type == 'slurm':
        client = slurm_dask_client(n_workers=n_workers,
                                   queue=queue, memory=memory)
    else:
        raise NotImplementedError

    futures = []
    for i in range(n_repeats):
        future = client.submit(run_calc,
                               exec_name,
                               inputs=[input_file,
                                       f'--prefix={prefix}{i:02d}',
                                       f'--symmetry={symmetry}',
                                       f'--mode={mode}'],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               shell=False, check=True)
        futures.append(future)
    return client, futures
