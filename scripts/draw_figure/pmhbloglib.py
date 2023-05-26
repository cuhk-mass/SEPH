import pandas
import os
import regex


def extract_log_info(filename):
    '''
        function: log file => experimental results (in dict)
        supported: 'MediaWrite_inMB', 'RTTP', 'RESIZEKV', 'DOUBLING', 'TailLatency', 'TotalRehashTime_inSecond',
                 'AverageRehashTime_inMicroSecond', 'LoadThroughput_inMops', 'RunThroughput_inMops', 'LoadFactor', 'RTTP_only'
    '''

    with open(filename, 'r') as f:
        log_lines = f.readlines()

    info_list = ['MediaWrite_inMB', 'RTTP', 'RESIZEKV', 'DOUBLING', 'TailLatency', 'TotalRehashTime_inSecond',
                 'AverageRehashTime_inMicroSecond', 'LoadThroughput_inMops', 'RunThroughput_inMops', 'LoadFactor', 'RTTP_only']

    result = {}
    for line in log_lines:
        for info in info_list:
            if (info + ' = ') in line:
                result[info] = eval(line[line.find('=')+1:])
                break

    return result


def full_name_map(file_name):
    if 'cceh_cow' in file_name:
        return "(EH-based) CCEH-C (Lock-Free)"
    if 'cceh' in file_name:
        return "(EH-based) CCEH"
    if 'clevel' in file_name:
        return "(Level-based) Clevel Hashing"
    if 'level' in file_name:
        return "(Level-based) Level Hashing"
    if 'dash' in file_name:
        return "(EH-based) Dash-EH"
    if 'steph' in file_name:
        return "SEPH"
    if 'pclht' in file_name:
        return "PCLHT"


full_name_order = ["PCLHT", "(Level-based) Level Hashing",
                   "(Level-based) Clevel Hashing", "(EH-based) CCEH", "(EH-based) CCEH-C (Lock-Free)", "(EH-based) Dash-EH", "SEPH"]


def short_name_map(file_name):
    if 'cceh_cow' in file_name:
        return "CCEH-C"
    if 'cceh' in file_name:
        return "CCEH"
    if 'clevel' in file_name:
        return "Clevel"
    if 'level' in file_name:
        return "Level"
    if 'dash' in file_name:
        return "Dash"
    if 'steph' in file_name:
        return "SEPH"
    if 'pclht' in file_name:
        return "PCLHT"


short_name_order = ["PCLHT", "Level",
                    "Clevel", "CCEH", "CCEH-C", "Dash", "SEPH"]


def rename_and_sort_artifacts(result, style):
    if style == 'full':
        namemap = full_name_map
        order = full_name_order
    else:
        namemap = short_name_map
        order = short_name_order

    renamed_result = {}
    for k, v in result.items():
        renamed_result[namemap(k)] = v

    sorted_result = []
    for k in order:
        if k in renamed_result:
            sorted_result.append((k, renamed_result[k]))

    return sorted_result


def drop_tail_1percent(ls):
    s = sum(ls)
    cur = 0
    while (cur + ls[-1]) * 100 < s:
        cur += ls[-1]
        ls.pop()
    return ls


def extract_RTTP_RESIZEKV(file):

    result = extract_log_info(file)

    rttp = result['RTTP']
    rttp = drop_tail_1percent(rttp)

    resize_kv = result['RESIZEKV']
    resize_kv = resize_kv[:len(rttp)]

    time_index = [x/10 for x in list(range(len(rttp)))]
    df = pandas.DataFrame([time_index, rttp, resize_kv])
    df = df.transpose()
    df.columns = [
        'time', 'throughput', 'rehash']

    return df


def extract_RTTP_only(file):
    result = extract_log_info(file)['RTTP_only']
    result = drop_tail_1percent(result)

    return result


def extract_RTTP_characteristic(file):
    result = drop_tail_1percent(extract_log_info(file)['RTTP_only'])
    result = [min(result), sum(result)/len(result), max(result)]

    return result


def extract_latency(file):
    result = extract_log_info(file)

    return result['TailLatency']


def extract_loadfactor(file):
    result = extract_log_info(file)

    return sum(result['LoadFactor'])/len(result['LoadFactor'])


def extract_rehashtime(file):
    result = extract_log_info(file)

    return result['TotalRehashTime_inSecond']


def extract_pmwrite(file):
    result = extract_log_info(file)

    return result['MediaWrite_inMB']


def extract_run_throughput(file):
    result = extract_log_info(file)

    return result['RunThroughput_inMops']


def cluster_scalability(results):
    pattern = '(\w+)_(\d+)\.txt'

    d = {}
    for k, v in results.items():
        match_result = regex.match(pattern, k)
        if match_result == None:
            print("unidentified filename: ", k)
        else:
            name = match_result.group(1)
            if name not in d:
                d[name] = []
            d[name].append((int(match_result.group(2)), float(v)*1e6))

    for k, v in d.items():
        d[k].sort()
        d[k] = [x[1] for x in d[k]]

    return d


def extract_from_directory(extract_function, dir):
    txt_files = [f for f in os.listdir(dir) if f.endswith('.txt')]

    results = {}
    for txt_file in txt_files:
        file_path = os.path.join(dir, txt_file)
        result = extract_function(file_path)
        results[txt_file] = result

    return results


def extract_scalability_from_directory(dir):
    result = extract_from_directory(extract_run_throughput, dir)
    result = cluster_scalability(result)

    return result
