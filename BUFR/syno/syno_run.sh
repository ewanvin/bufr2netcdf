#!/bin/bash -f
#$ -N wrapper
#$ -S /bin/bash
#$ -l h_rt=01:30:00
#$ -l h_vmem=100G
#$ -q research-el7.q
#$ -pe mpi 1
#$ -j y

source /modules/centos7/conda/Feb2021/etc/profile.d/conda.sh
conda activate secondenv

python /home/katarinana/work_desk/BUFR/syno/main_syno.py -c /home/katarinana/work_desk/BUFR/syno/syno_config.cfg -i -t state -a