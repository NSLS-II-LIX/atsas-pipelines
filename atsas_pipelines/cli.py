import argparse
import asyncio
import math

from .dask import (DEFAULT_MAXIMUM_WORKERS, DEFAULT_MEMORY,
                   DEFAULT_MINIMUM_WORKERS, DEFAULT_NUM_CORES, DEFAULT_QUEUE,
                   dask_slurm_cluster)


def run_cluster():
    description = 'Run a Dask Slurm cluster'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-q', '--queue', dest='queue',
                        default=DEFAULT_QUEUE, type=str,
                        help=(f'the Slurm queue to submit to. '
                              f'Default: {DEFAULT_QUEUE}'))
    parser.add_argument('-c', '--cores', dest='cores', type=int,
                        default=DEFAULT_NUM_CORES,
                        help=(f'the number of cores to use per job. '
                              f'Default: {DEFAULT_NUM_CORES}'))
    parser.add_argument('-m', '--memory', dest='memory', type=str,
                        default=DEFAULT_MEMORY,
                        help=(f'the amount of memory to use per job. '
                              f'Default: {DEFAULT_MEMORY}'))
    parser.add_argument('--minimum-workers', dest='minimum_workers', type=int,
                        default=DEFAULT_MINIMUM_WORKERS,
                        help=(f'the minimum number of workers to scale the '
                              f'cluster down to in the autoscale mode. '
                              f'Default: {DEFAULT_MINIMUM_WORKERS}'))
    parser.add_argument('--maximum-workers',
                        dest='maximum_workers', type=int,
                        default=DEFAULT_MAXIMUM_WORKERS,
                        help=(f'the maximum number of workers to scale the '
                              f'cluster up to in the autoscale mode. '
                              f'Default: {DEFAULT_MAXIMUM_WORKERS}'))

    args = parser.parse_args()
    cluster = dask_slurm_cluster(asynchronous=True, **args.__dict__)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(asyncio.sleep(math.inf))
    except Exception:
        cluster.close()


if __name__ == '__main__':
    run_cluster()
