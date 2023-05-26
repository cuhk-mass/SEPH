# fig7 rttp
insert_rttp = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'true',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

# fig8 latency + fig9 rz_time wr_count
insert_latency = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'true',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

# fig10 a
breakdown_sod = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}
breakdown_so = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'false',
    'BREAKDOWN_SO': 'true',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}
breakdown_s = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'false',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'true',
    'BREAKDOWN_BASE': 'false'
}
breakdown_base = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'false',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'true'
}

# fig10 b c
breakdown_sod_reason = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'true',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}
breakdown_so_reason = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'true',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'false',
    'BREAKDOWN_SO': 'true',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}
breakdown_s_reason = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'true',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'false',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'true',
    'BREAKDOWN_BASE': 'false'
}
breakdown_base_reason = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'true',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'false',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'true'
}

# fig9 load factor
load_factor = {
    'workload': 'workload_load',
    'thread_num': [48],
    "LOAD_FACTOR": 'true',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

# fig11
scalability_insert = {
    'workload': 'workload_load',
    'thread_num': [48, 24, 16, 8, 4, 2, 1],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

scalability_search = {
    'workload': 'workloadc',
    'thread_num': [48, 24, 16, 8, 4, 2, 1],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

scalability_update = {
    'workload': 'workload_update',
    'thread_num': [48, 24, 16, 8, 4, 2, 1],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

scalability_delete = {
    'workload': 'workload_delete',
    'thread_num': [48, 24, 16, 8, 4, 2, 1],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(1e7),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

# fig12
mixed_37 = {
    'workload': 'workload37',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

mixed_73 = {
    'workload': 'workload73',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

mixed_55 = {
    'workload': 'workload55',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

mixed_55_motivation = {
    'workload': 'workload55',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'true',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

# fig13
mixed_37_scalability = {
    'workload': 'workload37',
    'thread_num': [48, 24, 16, 8, 4, 2, 1],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

mixed_73_scalability = {
    'workload': 'workload73',
    'thread_num': [48, 24, 16, 8, 4, 2, 1],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

mixed_55_scalability = {
    'workload': 'workload55',
    'thread_num': [48, 24, 16, 8, 4, 2, 1],
    "LOAD_FACTOR": 'false',
    'load_num': int(1e7),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

# fig14
ycsb_load = {
    'workload': 'workload_load_big',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(0),
    'run_num': int(64e6),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}
ycsba = {
    'workload': 'workloada_big',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(64e6),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}
ycsbb = {
    'workload': 'workloadb_big',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(64e6),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}
ycsbc = {
    'workload': 'workloadc_big',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(64e6),
    'run_num': int(2e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}
ycsbd = {
    'workload': 'workloadd_big',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(64e6),
    'run_num': int(2e9),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}
ycsbf = {
    'workload': 'workloadf_big',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(64e6),
    'run_num': int(2.99e8),
    'LATENCY': 'false',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}

# fig15
ycsbd_latency = {
    'workload': 'workloadd_big',
    'thread_num': [48],
    "LOAD_FACTOR": 'false',
    'load_num': int(64e6),
    'run_num': int(2e9),
    'LATENCY': 'true',
    'COUNTING_WRITE': 'false',
    'BREAKDOWN_SOD': 'true',
    'BREAKDOWN_SO': 'false',
    'BREAKDOWN_S': 'false',
    'BREAKDOWN_BASE': 'false'
}


# all_configurations = ['load_factor']
all_configurations = ['insert_rttp', 'insert_latency']

all_configurations += ['scalability_insert', 'scalability_search',
                       'scalability_update', 'scalability_delete']

all_configurations += ['mixed_55_scalability', 'mixed_37_scalability',
                       'mixed_73_scalability']

all_configurations += ['mixed_55', 'mixed_37',
                       'mixed_73', 'mixed_55_motivation']

all_configurations += ['ycsb_load', 'ycsba',
                       'ycsbb', 'ycsbc', 'ycsbd', 'ycsbf']

all_configurations += ['ycsbd_latency']

all_configurations += ['load_factor']

all_configurations += ['breakdown_sod',
                       'breakdown_so', 'breakdown_s', 'breakdown_base']
all_configurations += ['breakdown_sod_reason', 'breakdown_so_reason',
                       'breakdown_s_reason', 'breakdown_base_reason']

# all_configurations = ['ycsb_load', 'ycsba',
#                       'ycsbb', 'ycsbc', 'ycsbf']
