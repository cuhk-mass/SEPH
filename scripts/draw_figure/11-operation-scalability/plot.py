#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import string
import openpyxl
import scienceplots

textwidth = 7
columnwidth = 3.34
# circle, triangle_up, square, pentagon, plus (filled), star, diamond
markers = ["o", "^", "s", "p", "H", "P", "*", "d"]
patterns = ['-', '//', '\\\\', 'x', 'xx', '-\\', '\\|', '']
plt.style.use(["default", "science", "nature", "../jlhu.mplstyle"])
colors = plt.rcParams["axes.prop_cycle"].by_key()['color']


if __name__ == "__main__":
    # read all sheets, one sheet for one subplot
    data = pd.read_excel("data.xlsx", sheet_name=None)
    fig, axes = plt.subplots(1, len(data))
    fig.set_size_inches(columnwidth, columnwidth/2.3)
    handles, labels = None, None
    for i, (name, ax) in enumerate(zip(data.keys(), axes)):
        sheet = data[name]
        x = sheet.iloc[:, 0]  # the first column is the thread num
        competitors = sheet.columns.tolist()[1:]
        for j, design in enumerate(competitors):
            ax.plot(x, sheet[design], marker=markers[j])
        handles = ax.get_lines()
        labels = competitors
        ax.set_xticks(x[3:])
        ax.set_xlabel(f"Num. of Threads\n({string.ascii_lowercase[i]}) {name}")
        ax.ticklabel_format(axis='y', style='sci', scilimits=(
            6, 6), useMathText=True)  # set SCI format in Y axis
        # ax.set_ylim(0, df.max().max()) # the maximum value in the table
    axes[0].set_ylabel('Average Throughput (ops)')
    fig.legend(handles, labels, ncol=len(labels), handletextpad=0.2, columnspacing=.5, fontsize="small",
               bbox_to_anchor=(0.5, 1.0), loc='lower center')
    fig.tight_layout(pad=0)
    fig.savefig('plot.pdf', dpi=1000, bbox_inches='tight')
    plt.cla()
