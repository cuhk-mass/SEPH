import os
import pandas
import sys
sys.path.append('../')

from pmhbloglib import *  # NOQA: E402

sheet_names = ['S30%% I70%%', 'S50%% I50%%', 'S70%% I30%%']
dir_names = ['mixed_37', 'mixed_55', 'mixed_73']


def main():
    writer = pandas.ExcelWriter('./data.xlsx', engine='xlsxwriter')

    for i in range(len(sheet_names)):
        result = dict(rename_and_sort_artifacts(
            extract_from_directory(extract_RTTP_only, '../../../data/' + dir_names[i]), 'short'))

        max_len = max([len(result[k]) for k in result])
        for k in result:
            if len(result[k]) != max_len:
                result[k].extend(['']*(max_len-(len(result[k]))))

        df = pandas.DataFrame(result, index=[x/10 for x in range(max_len)])
        df.index.name = 'time'
        df.to_excel(writer, sheet_name=sheet_names[i])

    writer.close()


main()
