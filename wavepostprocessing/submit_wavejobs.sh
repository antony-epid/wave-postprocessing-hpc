#!/bin/bash
#SBATCH --output=/rfs/project/rfs-Bl26eNcUDB8/users_writeable/slurm_logs/%u-%A-%a.out
#SBATCH --error=/rfs/project/rfs-Bl26eNcUDB8/users_writeable/slurm_logs/%u-%A-%a.err
#SBATCH --ntasks-per-node=1
#SBATCH --account=BRAGE-SL3-CPU
. /etc/profile.d/modules.sh                # Leave this line (enables the module command)
module purge                               # Removes all modules still loaded
module load rhel8/default-icl              # REQUIRED - loads the basic environment


set -eu #exit on error

set +u

#RUNPATH="/rfs/project/rfs-Bl26eNcUDB8/users_writeable/"
#cd $RUNPATH

echo "${SLURM_ARRAY_TASK_ID} ${SLURM_ARRAY_TASK_COUNT}"
echo "srun python -m $@ ${SLURM_ARRAY_TASK_ID} ${SLURM_ARRAY_TASK_COUNT}"

#cat $3

#source "/rfs/project/rfs-Bl26eNcUDB8/users_writeable/python3/bin/activate" #setup virtual environment 
#source "/rfs/project/rfs-Bl26eNcUDB8/users_writeable/antony_runs/Venvs/wave-postproc-venv/bin/activate"
#source "/rfs/project/rfs-Bl26eNcUDB8/users_writeable/venvs/Waveppenv/bin/activate"

#activate the virtual environment ported by the launcher script
args=("$@")
source "${args[-1]}"

#now remove the virtual environment from the arguments
unset 'args[-1]'


#cd /rfs/project/rfs-Bl26eNcUDB8/users_writeable/antony_runs/Repos/Wave_PostProcessing/wavepostprocessing
#-----------catch the job id and change the status file into running ?
#srun python $@ ${SLURM_ARRAY_TASK_ID} ${SLURM_ARRAY_TASK_COUNT} #start python with script and args specified on sbatch line
#srun python $@ 1 1 #start python with script and args specified on sbatch line
#srun python $@ ${SLURM_ARRAY_TASK_ID} ${SLURM_ARRAY_TASK_COUNT} #start python with script and args specified on sbatch line
#srun python -m $@ ${SLURM_ARRAY_TASK_ID} ${SLURM_ARRAY_TASK_COUNT} #start python with script and args specified on sbatch line
srun python -m ${args[@]} ${SLURM_ARRAY_TASK_ID} ${SLURM_ARRAY_TASK_COUNT} #start python with script and args specified on sbatch line

#srun python -m $@ ${SLURM_ARRAY_TASK_ID} ${SLURM_ARRAY_TASK_COUNT} #start python with script and args specified on sbatch line
#srun python $@ 4 20 #start python with script and args specified on sbatch line
#catch the job id and move the status file ?

set -u
