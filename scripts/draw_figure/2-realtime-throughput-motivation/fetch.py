import os
import pandas
import sys
sys.path.append('../')

from pmhbloglib import *  # NOQA: E402


def remove_key_for_motivation(d):
    removed_name = ["PCLHT", "(EH-based) CCEH-C (Lock-Free)",  "SEPH"]
    for k in removed_name:
        if k in d:
            del d[k]
    return d


def main():
    result = extract_from_directory(
        extract_RTTP_RESIZEKV, '../../../data/mixed_55_motivation')
    result = dict(rename_and_sort_artifacts(result, 'full'))

    remove_key_for_motivation(result)

    writer = pandas.ExcelWriter('./data.xlsx', engine='xlsxwriter')
    for k, data in result.items():
        data.to_excel(writer, sheet_name=k,
                      index=False)
    writer.close()


main()
