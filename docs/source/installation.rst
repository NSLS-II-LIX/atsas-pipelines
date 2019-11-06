************
Installation
************

Prerequisites
=============

Install the ATSAS software package for your system from
https://www.embl-hamburg.de/biosaxs/atsas-online/download.php after
registration.


Preparation
=============

Create a conda environment::

    $ conda create -y -n atsas python=3.7 ipython
    $ conda activate atsas  # or 'source activate atsas'


Install the ``atsas-pipelines`` package
=======================================

PyPI
----

Install from PyPI::

    $ pip install atsas-pipelines

Conda
-----

Install from conda::

    $ conda install -c nsls2forge atsas-pipelines

Development mode
----------------

For the development mode, follow the steps below to install the
``atsas-pipelines`` package from source::

    $ git clone https://github.com/mrakitin/atsas-pipelines
    $ cd atsas-pipelines
    $ pip install -r requirements-dev.txt
    $ pip install -ve .
