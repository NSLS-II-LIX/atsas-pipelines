import sys
import argparse
import logging

from distributed.cli.utils import check_python_3, install_signal_handlers
from tornado.ioloop import IOLoop

from .dask import (DEFAULT_ADDRESS, DEFAULT_MAXIMUM_WORKERS, DEFAULT_MEMORY,
                   DEFAULT_MINIMUM_WORKERS, DEFAULT_NUM_CORES, DEFAULT_PORT,
                   DEFAULT_QUEUE, dask_slurm_cluster)


logger = logging.getLogger('atsas-pipelines')


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
    parser.add_argument('--address', dest='address', type=str,
                        default=DEFAULT_ADDRESS,
                        help=(f"the network address to be assigned to the "
                              f"cluster's scheduler. "
                              f'Default: {DEFAULT_ADDRESS}'))
    parser.add_argument('--port', dest='port', type=str,
                        default=DEFAULT_PORT,
                        help=(f"the network port to be assigned to the "
                              f"cluster's scheduler. "
                              f'Default: {DEFAULT_PORT}'))
    args = parser.parse_args()

    cluster = dask_slurm_cluster(**args.__dict__)

    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info(f'Starting cluster {cluster.__repr__()}\n{args.__dict__}')

    loop = IOLoop.current()
    install_signal_handlers(loop)

    async def run():
        await cluster
        await cluster.scheduler.finished()

    try:
        loop.run_sync(run)
    finally:
        logger.info(f'End cluster {cluster.__repr__()}')
        cluster.close()


def go():
    check_python_3()
    run_cluster()


if __name__ == "__main__":
    go()
