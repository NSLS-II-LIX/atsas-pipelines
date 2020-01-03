#!/bin/bash

. /opt/conda/etc/profile.d/conda.sh
conda activate /GPFS/APC/mrakitin/conda_envs/atsas

# . /opt/apps/lmod/lmod/init/bash
. /etc/profile.d/z00_lmod.sh
module load ATSAS
module load StdEnv
module load slurm

dask-slurm-cluster --address=$(hostname)
