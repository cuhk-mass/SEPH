import os
import sys

from scripts.launcher import *


if sys.argv[1] in ['launch_all', 'check', 'complete']:
    launcher(sys.argv[1], '')
elif len(sys.argv) > 2:
    for config_name in sys.argv[2:]:
        launcher(sys.argv[1], config_name)
else:
    print(f"{sys.argv[1]}")
    th_num = 48
    if sys.argv[1] in ['steph', 'clevel'] and th_num == 48:
        th_num -= 1
    # trace = 'workload_uniform_2e9'
    # trace = 'workload37'
    # load_num = int(2e9)
    # run_num = int(2e9)

    trace = 'workload_load'
    # # trace = 'workloada'
    # # trace = 'workload55'
    load_num = int(1e7)
    run_num = int(2e8)

    # # for big load
    # # trace = 'workloada_big_preload3'
    # trace = 'workloadf_big'
    # load_num = int(64e6)
    # run_num = int(2.99e8)

    # trace = 'workload_delete'
    # # # trace = 'workloada'
    # # # trace = 'workload55'
    # load_num = int(1e7)
    # run_num = int(1e7)

    # # trace = 'workload55'
    # # trace = 'workload_update'
    # # trace = 'workload_search'
    # # load_num = int(1e8) if trace in [
    # #     'workload_search', 'workload_update', 'workload_delete', 'workload_load'] else int(1e7)
    # # run_num = int(1e8) if trace not in ['workload_load'] else int(0)

    if not os.path.exists('./data/'):
        os.mkdir('./data')
    if not os.path.exists('./data/tmp'):
        os.mkdir('./data/tmp')

    os.system(
        f'numactl -N0 ./builddir/pmhb -w /mnt/pmem0/Testee/ -o ./data/tmp '
        f'-l /home/cwang/load/{trace}.ycsb '
        f'-r /home/cwang/run/{trace}.ycsb '
        f'-m /mnt/pmem0/Trace/{trace} '
        f'--load_num {load_num} --run_num {run_num} '
        f'-e {sys.argv[1]} -t {th_num} '
        f'> ./data/tmp/{sys.argv[1]}_{th_num}.txt'
    )

print(sys.argv)
# an example to test a single design with a single config, but build pmhb with correct Marcos first:
f"./builddir/pmhb -w /mnt/pmem0/Testee/ -o ./data/tmp -l /home/cwang/load/workload_load.ycsb -r /home/cwang/run/workload_load.ycsb -m /mnt/pmem0/Trace/workload_load --load_num 10000000 --run_num 20000000 -e speth -t 47"
