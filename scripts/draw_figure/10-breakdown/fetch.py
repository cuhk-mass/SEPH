import os
import pandas
import sys
sys.path.append('../')

from pmhbloglib import *  # NOQA: E402

sheet_names = [
    'MinAvgMax Throughput', 'Total Bytes Written to PM', 'Total Resizing Time (s)']

column_names = ['Base', 'S', 'SO', 'SOD']

original = ['breakdown_base', 'breakdown_s', 'breakdown_so', 'breakdown_sod']
reason = ['breakdown_base_reason', 'breakdown_s_reason',
          'breakdown_so_reason', 'breakdown_sod_reason']


def fetch_throughput():
    characteristics = []
    for x in original:
        file = '../../../data/'+x+'/steph_47.txt'
        characteristics.append(extract_RTTP_characteristic(file))

    result = {}
    for i, v in enumerate(characteristics):
        result[column_names[i]] = v

    return result


def fetch_reason(reason_name):
    reasons = []
    for x in reason:
        file = '../../../data/'+x+'/steph_47.txt'
        reasons.append([extract_log_info(file)[reason_name]])

    result = {}
    for i, v in enumerate(reasons):
        result[column_names[i]] = v

    return result


def main():
    writer = pandas.ExcelWriter('./data.xlsx', engine='xlsxwriter')

    characteristics = fetch_throughput()
    df = pandas.DataFrame(characteristics, index=[
                          'Min', 'Avg', 'Max'])
    df.to_excel(writer, sheet_name=sheet_names[0])

    pmwrite = fetch_reason('MediaWrite_inMB')
    for k in pmwrite:
        pmwrite[k][0] *= 1024*1024  # MB->B
    df = pandas.DataFrame(pmwrite, index=['B'])
    df.to_excel(writer, sheet_name=sheet_names[1])

    rehashtime = fetch_reason('TotalRehashTime_inSecond')
    df = pandas.DataFrame(rehashtime, index=['S'])
    df.to_excel(writer, sheet_name=sheet_names[2])

    writer.close()


main()
