import subprocess

from dask.distributed import Client

from .utils import find_executable


def run_command(exec_name, inputs=None, *args, **kwargs):
    """
    Run a command with the specified executable.

    Parameters
    ----------
    exec_name : str
        the name of the executable
    inputs : list
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


def run_with_dask(client, exec_name, input_file, prefix='test', symmetry='P1',
                  mode='FAST', n_repeats=1, wait=False):
    """
    Run parallel jobs with Dask.

    Parameters
    ----------
    client : dask.distributed.Client
        an instance of the Dask client
    exec_name : str
        the name of the executable
    input_file : str
        the name of the input file. Example: ``examples/IgG_0152-0159s.out``
    prefix : str, optional
        dammif input: the prefix to prepend to any output filename (default:
        dammif)
    symmetry : str, optional
        dammif input: particle symmetry
    mode : str, optional
        dammif input: one of: FAST, SLOW, INTERACTIVE (default: interactive)
    n_repeats : int, optional
        a number of repeats of a simulation (to collect statistics, the
        randomness is comming from the program itself)
    wait : bool, optional
        wait until all the jobs are finished

    dammif help
    -----------
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
    futures = []
    for i in range(n_repeats):
        future = client.submit(run_command,
                               exec_name,
                               inputs=[input_file,
                                       f'--prefix={prefix}{i:02d}',
                                       f'--symmetry={symmetry}',
                                       f'--mode={mode}'],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT,
                               shell=False, check=True)
        futures.append(future)

    if wait:
        futures = client.gather(futures)

    return futures
