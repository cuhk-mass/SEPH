import os
gen = "./bin/ycsb.sh"

trace_root = '../Trace_AE'
dir_names = [f'{trace_root}', f'{trace_root}/load', f'{trace_root}/run']

for d in dir_names:
    if not os.path.exists(d):
        os.mkdir(d)

profiles = ['workload37', 'workload73', 'workload_load', 'workloada_big', 'workloadc', 'workloadd_big',
            'workload55', 'workload_delete', 'workload_load_big', 'workload_update', 'workloadb_big', 'workloadc_big', 'workloadf_big']


for p in profiles:
    for m in ['load', 'run']:
        command = f'{gen} {m} basic -P workloads/{p} > {trace_root}/{m}/{p}.ycsb'
        if p != 'workload_delete':
            command += ' &'
        else:
            if m == 'load':
                # construct delete workload because YCSB doesn't support delete.
                command += f" && sed 's/INSERT/DELETE/g' {trace_root}/{m}/{p}.ycsb > {trace_root}/run/{p}.ycsb &"
            else:
                continue
        print(command)
        os.system(command)
