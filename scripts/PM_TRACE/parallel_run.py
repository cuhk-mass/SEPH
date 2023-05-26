import os

if not os.path.exists('logs'):
    os.mkdir('logs')

cmd = "./builddir/trace_maker"
# profiles = ['workload_load_uniform_2e9']

profiles = ['workloadc', 'workload_load',
            'workload_update', 'workload_delete']
profiles += ['workload37',
             'workload55', 'workload73']
profiles += ['workload_load_big', 'workloada_big', 'workloadb_big',
             'workloadc_big', 'workloadd_big', 'workloadf_big']
for profile in profiles:
    cmd_instance = f"{cmd} /home/cwang/Trace/load/{profile}.ycsb /home/cwang/Trace/run/{profile}.ycsb /mnt/pmem0/Trace/{profile} > logs/{profile}.log &"
    print(cmd_instance)
    os.system(cmd_instance)
