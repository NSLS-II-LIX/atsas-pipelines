import subprocess
from .utils import find_executable
from dask.distributed import Client


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
    print(exec_path)

    cmd = [exec_path] + inputs
    
    print(cmd)

    st = subprocess.run(cmd, *args, **kwargs)
    return st


def run_with_dask(exec_name, 
                  input_file, prefix='test', symmetry='P1', mode='FAST',
                  n_repeats=1,
                  threads_per_worker=4, n_workers=1):
    client = Client(threads_per_worker=threads_per_worker,
                    n_workers=n_workers)

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
