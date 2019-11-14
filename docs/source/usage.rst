=====
Usage
=====

Run ATSAS tools in command line
===============================

Run the ATSAS simulation directly (assuming you are in the cloned repo dir).

.. code-block:: bash

    $ dammif examples/IgG_0152-0159s.out --prefix=test --symmetry=P1 --mode=FAST


Run ATSAS tools from Python using Dask
======================================

Start an IPython session with ``ipython`` to perform the following
calculations.


Local cluster
-------------

Run 36 separate ATSAS simulations on a local Dask cluster.

.. code-block:: python

    from atsas_pipelines.dask import dask_client
    from atsas_pipelines.run import run_with_dask

    client = dask_client()
    futures = run_with_dask('dammif', 'examples/IgG_0152-0159s.out',
                            n_repeats=36)
    client.gather(futures)
    fut = futures[0]
    fut.result()
    out = fut.result().stdout.decode('utf-8')
    print(out)


Slurm cluster
-------------

.. code-block:: python

    from atsas_pipelines.dask import dask_client, dask_slurm_cluster
    from atsas_pipelines.run import run_with_dask

    cluster = dask_slurm_cluster()
    client = dask_client(cluster)

    futures = run_with_dask('dammif', 'examples/IgG_0152-0159s.out',
                            n_repeats=36)

See :func:`~atsas_pipelines.run.run_with_dask` for implementation.
