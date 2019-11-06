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

Asynchronous mode
-----------------

Run 36 separate ATSAS simulations on 12 workers of a local Dask cluster in an
asynchronous mode.

.. code-block:: python

    from atsas_pipelines.run_calc import run_with_dask
    client, futures = run_with_dask('dammif', 'examples/IgG_0152-0159s.out',
                                    n_repeats=36, n_workers=12)
    client.gather(futures)
    fut = futures[0]
    fut.result()
    out = fut.result().stdout.decode('utf-8')
    print(out)
    client.cluster.close()

Synchronous mode (local cluster)
--------------------------------

.. code-block:: python

    from atsas_pipelines.run_calc import run_with_dask
    client, futures = run_with_dask('dammif', 'examples/IgG_0152-0159s.out',
                                    n_repeats=36, n_workers=12,
                                    dask_client_type='local', wait=True)

Synchronous mode (Slurm cluster)
--------------------------------

.. code-block:: python

    from atsas_pipelines.run_calc import run_with_dask
    client, futures = run_with_dask('dammif', 'examples/IgG_0152-0159s.out',
                                    n_repeats=36, n_workers=12,
                                    dask_client_type='slurm', wait=True)


See :func:`~atsas_pipelines.run_calc.run_with_dask` for implementation.
