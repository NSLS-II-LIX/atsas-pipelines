============
Installation
============

Install the ATSAS software package for your system from
https://www.embl-hamburg.de/biosaxs/atsas-online/download.php after
registration.

Create a conda environment::

    $ conda create -y -n atsas python=3.7
    $ conda activate atsas  # or 'source activate atsas'

Install the ``atsas-pipelines`` package from source::

    $ git clone https://github.com/mrakitin/atsas-pipelines
    $ cd atsas-pipelines
    $ pip install -r requirements-dev.txt
    $ pip install -ve .
