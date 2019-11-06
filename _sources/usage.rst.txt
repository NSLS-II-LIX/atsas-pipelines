=====
Usage
=====

Run the 32 separate ATSAS simulations on 12 Dask workers.

.. code-block:: python

    from atsas_pipelines.run_calc import run_with_dask
    client, futures = run_with_dask('dammif', 'examples/IgG_0152-0159s.out',
                                    n_repeats=36, threads_per_worker=1,
                                    n_workers=12)
    client.gather(futures)
    fut = futures[0]
    fut.result()
    out = fut.result().stdout.decode('utf-8')
    print(out)
