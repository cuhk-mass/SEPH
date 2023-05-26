import os
import pandas
import sys
sys.path.append('../')

from pmhbloglib import *  # NOQA: E402

sheet_names = ['Workload Load \nI100p', 'Workload A \nS50p U50p', 'Workload B \nS95p U5p',
               'Workload C \nS100p', 'Workload D \nS95p I5p', 'Workload F \nS95p RMW5p']

dir_names = ['ycsb_load', 'ycsba',
             'ycsbb', 'ycsbc', 'ycsbd', 'ycsbf']


def main():
    writer = pandas.ExcelWriter('./data.xlsx', engine='xlsxwriter')

    for i in range(len(sheet_names)):
        characteristics = dict(rename_and_sort_artifacts(extract_from_directory(
            extract_RTTP_characteristic, '../../../data/'+dir_names[i]), 'short'))
        df = pandas.DataFrame(characteristics, index=[
            'Min', 'Avg', 'Max'])
        df.to_excel(writer, sheet_name=sheet_names[i])

    writer.close()


main()
