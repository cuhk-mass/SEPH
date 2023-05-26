import os
import pandas
import sys
sys.path.append('../')

from pmhbloglib import *  # NOQA: E402


def main():
    names = [
        'Total Resizing Time (s)', 'Total Bytes Written to PM (B)', 'Loadfactor']
    totalresizingtime = dict(rename_and_sort_artifacts(extract_from_directory(
        extract_rehashtime, '../../../data/insert_rttp'), 'short'))
    totalpmwrite = dict(rename_and_sort_artifacts(extract_from_directory(
        extract_pmwrite, '../../../data/insert_rttp'), 'short'))
    for k in totalpmwrite:
        totalpmwrite[k] *= 1024 * 1024  # MB -> B
    totalpmwrite['Expected'] = 51200000000
    loadfactor = dict(rename_and_sort_artifacts(extract_from_directory(
        extract_loadfactor, '../../../data/load_factor'), 'short'))

    writer = pandas.ExcelWriter('./data.xlsx', engine='xlsxwriter')
    for i, x in enumerate([totalresizingtime, totalpmwrite, loadfactor]):
        for k in x:
            x[k] = [x[k]]
        df = pandas.DataFrame(x, index=None)
        df.to_excel(writer, sheet_name=names[i], index=False)

    writer.close()


main()
