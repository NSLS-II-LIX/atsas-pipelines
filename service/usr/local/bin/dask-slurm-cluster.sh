#!/bin/bash

. /opt/conda/etc/profile.d/conda.sh
conda activate /GPFS/APC/mrakitin/conda_envs/atsas

. /etc/profile.d/z00_lmod.sh
module load StdEnv
module load slurm
module load ATSAS

dask-slurm-cluster --address=$(hostname)
