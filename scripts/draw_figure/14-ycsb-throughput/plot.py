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
    fig, axes = plt.subplots(2, len(data)//2)
    axes = axes.flatten()
    fig.set_size_inches(columnwidth, columnwidth/1.3)
    for i, (name, ax) in enumerate(zip(data.keys(), axes)):
        # sheet = data[name]
        # x = sheet.iloc[:, 0]
        # competitors = sheet.columns.tolist()[1:]
        # for j, design in enumerate(competitors):
        #     ax.plot(x, sheet[design], marker=markers[j])
        # handles = ax.get_lines()
        # labels = competitors
        # ax.set_xticks(x[3:])
        # ax.set_xlabel(f"({string.ascii_lowercase[i]}) {name}", fontsize="small")
        # ax.ticklabel_format(axis='y', style='sci', scilimits=(6, 6), useMathText=True)
        # # ax.set_ylim(0, df.max().max()) # the maximum value in the table

        sheet = data[name]
        competitors = sheet.columns.tolist()[1:]
        x = sheet.columns.tolist()[1:]
        min = sheet.iloc[0, 1:]
        avg = sheet.iloc[1, 1:]
        max = sheet.iloc[2, 1:]
        labels = competitors
        handles = []
        for j, v in enumerate(x):
            up = max[j] - avg[j]
            lo = avg[j] - min[j]
            # print(up, avg[j], lo)
            elem = ax.errorbar(x[j], avg[j], yerr=[
                               [lo], [up]], color=colors[j], marker=markers[j], capsize=2, linestyle='none')
            handles.append(elem)
        # ax.ticklabel_format(axis='y', style='sci', scilimits=(7, 7), useMathText=True)
        ax.set_xticklabels([])
        ax.set_xlabel(f"({string.ascii_lowercase[i]}) " + name.replace(
            "n", "\n").replace("p", r"\%"), multialignment='center')
        # ax.set_xlabel(f"({string.ascii_lowercase[i]}) {name}", fontsize="small")
        ax.set_ylim(0)  # the maximum value in the table

    axes[0].set_ylabel('Average Throughput (ops)', ha="right")
    # fig.legend(handles, labels, ncol=len(labels),
    #            bbox_to_anchor=(0.5, 1.0), loc='lower center',
    #            handletextpad=0.2, columnspacing=.5, fontsize="small"))
    fig.legend(handles, labels, ncol=len(labels), handletextpad=0.2, columnspacing=.5, fontsize="small",
               bbox_to_anchor=(0.5, .97), loc='lower center', numpoints=1)
    fig.tight_layout(pad=.5)
    fig.savefig('plot.pdf', dpi=1000, bbox_inches='tight')
    plt.cla()
