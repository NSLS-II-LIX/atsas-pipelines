from dask_jobqueue import SLURMCluster
from dask.distributed import Client


DEFAULT_QUEUE= 'lix-atsas'
DEFAULT_NUM_CORES = 1
DEFAULT_MEMORY = '4GB'
DEFAULT_MINIMUM_WORKERS = 0
DEFAULT_MAXIMUM_WORKERS = 20


def dask_slurm_cluster(queue=None, cores=None, memory=None,
                       minimum_workers=None, maximum_workers=None):
    queue = queue or DEFAULT_QUEUE
    cores = cores or DEFAULT_NUM_CORES
    memory = memory or DEFAULT_MEMORY
    minimum_workers = minimum_workers or DEFAULT_MINIMUM_WORKERS
    maximum_workers = maximum_workers or DEFAULT_MAXIMUM_WORKERS

    cluster = SLURMCluster(queue=queue, cores=cores, memory=memory)
    cluster.adapt(minimum=minimum_workers, maximum=maximum_workers)
    return cluster


def dask_client(cluster=None):
    args = []
    if cluster is not None:
        args.append(cluster)
    client = Client(*args)
    return client
