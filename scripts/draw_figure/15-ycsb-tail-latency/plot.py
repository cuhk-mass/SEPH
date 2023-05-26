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
    fig.set_size_inches(columnwidth, columnwidth/2.5)
    handles, labels = None, None
    for i, (name, ax) in enumerate(zip(data.keys(), [axes])):
        sheet = data[name]
        x = sheet.iloc[:, 0]  # the first column is the percentile
        competitors = sheet.columns.tolist()[1:]
        for j, design in enumerate(competitors):
            ax.plot(x, sheet[design], marker=markers[j])
        ax.set_yscale('log')
        ax.set_ylabel("Latency (ns)")
        # ax.set_xlabel(f"{name}")
        ax.set_xticks(x, x, rotation=15)
        ax.legend(ax.get_lines(), competitors, ncol=2)
    fig.tight_layout(pad=0)
    fig.savefig('plot.pdf', dpi=1000, bbox_inches='tight')
    plt.cla()
