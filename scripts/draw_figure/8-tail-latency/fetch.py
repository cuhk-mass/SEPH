import os
import pandas
import sys
sys.path.append('../')

from pmhbloglib import *  # NOQA: E402


def main():

    result = extract_from_directory(
        extract_latency, '../../../data/insert_rttp')

    result = rename_and_sort_artifacts(result, 'short')

    idx = ['50%#', '75%#', '90%#', '99%#',
           '99.9%#', '99.99%#', '99.999%#', '100%#']

    df = pandas.DataFrame(dict(result), index=idx)
    df.to_excel('./data.xlsx', sheet_name="Sheet1")

    # print(df)


main()
