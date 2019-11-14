import argparse

from .dask import (DEFAULT_MAXIMUM_WORKERS, DEFAULT_MEMORY,
                   DEFAULT_MINIMUM_WORKERS, DEFAULT_NUM_CORES, DEFAULT_QUEUE,
                   dask_slurm_cluster)


def run_cluster():
    description = 'Run a Dask Slurm cluster'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-q', '--queue', dest='queue',
                        default=DEFAULT_QUEUE, type=str,
                        help='a Slurm queue to submit to')
    parser.add_argument('-c', '--cores', dest='cores', type=int,
                        default=DEFAULT_NUM_CORES,
                        help='number of cores to use per job')
    parser.add_argument('-m', '--memory', dest='memory', type=str,
                        default=DEFAULT_MEMORY,
                        help='amount of memory to use per job')
    parser.add_argument('--minimum-workers', dest='minimum_workers', type=int,
                        default=DEFAULT_MINIMUM_WORKERS,
                        help='minimum number of workers in the autoscale mode')
    parser.add_argument('--maximum-workers', dest='maximum_workers', type=int,
                        default=DEFAULT_MAXIMUM_WORKERS,
                        help='maximum number of workers in the autoscale mode')

    args = parser.parse_args()
    cluster = dask_slurm_cluster(**args.__dict__)
    return cluster


if __name__ == '__main__':
    cluster = run_cluster()
