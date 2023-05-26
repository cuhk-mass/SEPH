import os
import pandas
import sys
sys.path.append('../')

from pmhbloglib import *  # NOQA: E402


sheet_names = ['Insertion', 'Update', 'Deletion', 'Search']
dir_names = ['scalability_insert', 'scalability_update',
             'scalability_delete', 'scalability_search']


def main():
    writer = pandas.ExcelWriter('./data.xlsx', engine='xlsxwriter')

    for i in range(len(sheet_names)):
        result = extract_scalability_from_directory(
            '../../../data/'+dir_names[i])

        result = dict(rename_and_sort_artifacts(result, 'short'))

        df = pandas.DataFrame(result, index=[1, 2, 4, 8, 16, 32, 48])

        df.to_excel(writer, sheet_name=sheet_names[i])

    writer.close()


main()
