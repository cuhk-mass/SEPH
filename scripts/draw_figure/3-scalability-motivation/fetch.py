import os
import pandas
import sys
sys.path.append('../')

from pmhbloglib import *  # NOQA: E402


sheet_names = ['Average Throughput S50%# I50%#', 'Total Bytes Written to PM']
dir_names = ['mixed_55_scalability', 'mixed_55_motivation']


def remove_key_for_motivation(d):
    removed_name = ["PCLHT", "CCEH-C",  "SEPH"]
    for k in removed_name:
        if k in d:
            del d[k]
    return d


def main():
    writer = pandas.ExcelWriter('./data.xlsx', engine='xlsxwriter')

    # scalability
    result = extract_scalability_from_directory(
        '../../../data/'+dir_names[0])
    result = dict(rename_and_sort_artifacts(result, 'short'))
    remove_key_for_motivation(result)

    df = pandas.DataFrame(result, index=[1, 2, 4, 8, 16, 32, 48])
    df.to_excel(writer, sheet_name=sheet_names[0])

    # write
    result = extract_from_directory(
        extract_pmwrite, '../../../data/'+dir_names[1])
    result = dict([(x[0], x[1]*1024*1024) for x in rename_and_sort_artifacts(
        result, 'short')]+[('Exptected', 24414062.5)])
    remove_key_for_motivation(result)

    df = pandas.DataFrame(result, index=[0])
    df.to_excel(writer, sheet_name=sheet_names[1])

    writer.close()


main()
