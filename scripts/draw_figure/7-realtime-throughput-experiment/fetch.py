import os
import pandas
import sys
sys.path.append('../')

from pmhbloglib import *  # NOQA: E402


def main():

    result = extract_from_directory(
        extract_RTTP_RESIZEKV, '../../../data/insert_rttp')
    result = rename_and_sort_artifacts(result, 'full')

    writer = pandas.ExcelWriter('./data.xlsx', engine='xlsxwriter')
    for k, data in result:
        data.to_excel(writer, sheet_name=k,
                      index=False)
    writer.close()


main()
