#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import string
import openpyxl
from itertools import chain
import scienceplots

textwidth = 7
columnwidth = 3.5
# circle, triangle_up, square, pentagon, plus (filled), star, diamond
markers = ["o", "^", "s", "p", "P", "*", "D"]
patterns = ['-', '//', '\\\\', 'X', 'xx', '-\\', '\\|', '']
plt.style.use(["default", "science", "nature", "../jlhu.mplstyle"])
colors = plt.rcParams["axes.prop_cycle"].by_key()['color']

markers = list(chain(markers[3:]))
colors = list(chain(colors[1:2], colors[4:]))
colors = ['#f6ce98d0', '#f2b276d0', '#ee914fd0', '#FF2C00D0']
colors = ['#ff9900d0', '#ff7700d0', '#ff5100d0', '#FF2C00D0']

if __name__ == "__main__":
    # read all sheets, one sheet for one subplot
    data = pd.read_excel("data.xlsx", sheet_name=None, index_col=None)
    fig, (ax0, ax1, ax2) = plt.subplots(
        1, 3, gridspec_kw={'width_ratios': [4, 4, 4]})
    (name0, name1, name2) = data.keys()
    fig.set_size_inches(columnwidth, columnwidth/2.3)

    sheet = data[name0]
    x = sheet.columns.to_list()[1:]
    min = sheet.iloc[0, 1:]
    avg = sheet.iloc[1, 1:]
    max = sheet.iloc[2, 1:]
    for i, v in enumerate(x):
        up = max[i] - avg[i]
        lo = avg[i] - min[i]
        ax0.errorbar(x[i], avg[i], yerr=[[lo], [up]], color=colors[i],
                     marker=markers[i], capsize=2, linestyle='none')
    ax0.set_ylim((0, 25e6))
    ax0.ticklabel_format(axis='y', style='sci',
                         scilimits=(6, 6), useMathText=True)
    ax0.set_xlabel(f"(a) Min/Avg/Max Throughput", fontsize="small")
    ax0.set_ylabel(f"Throughput (ops)", fontsize="small", labelpad=1)
    ax0.tick_params(axis='x', which='major', labelsize=6)

    sheet = data[name1]
    sheet.index = sheet.iloc[:, 0]
    colordict = dict(
        zip(sheet.columns[1:], colors))
    sheet.iloc[:, 1:].transpose().plot(
        ax=ax1, kind='bar', color=colors, width=1., rot=0, legend=False)
    bars = ax1.patches
    edgecolor = [c for c in colors for _ in range(len(sheet))]
    hatches = [p for p in patterns for _ in range(len(sheet))]
    for bar, hatch, ec in zip(bars, hatches, edgecolor):
        bar.set_edgecolor(ec)
        bar.set_hatch(hatch)
        bar.set(fill=False)
        ax1.annotate(f"{bar.get_height()/1e10:.1f}", (bar.get_x() + bar.get_width()/5,
                     bar.get_height() + ax1.get_ylim()[-1]*0.025), color=ec, fontsize="small")
    ax1.set_xlabel(f"(b) {name1}", fontsize="small")
    ax1.ticklabel_format(axis='y', style='sci',
                         scilimits=(10, 10), useMathText=True)
    ax1.tick_params(axis='x', which='major', labelsize=6)
    ax1.set_ylim((0, 20e10))

    sheet = data[name2]
    sheet.index = sheet.iloc[:, 0]
    colordict = dict(
        zip(sheet.columns[1:], colors))
    sheet.iloc[:, 1:].transpose().plot(
        ax=ax2, kind='bar', color=colors, width=1., rot=0, legend=False)
    bars = ax2.patches
    edgecolor = [c for c in colors for _ in range(len(sheet))]
    hatches = [p for p in patterns for _ in range(len(sheet))]
    for bar, hatch, ec in zip(bars, hatches, edgecolor):
        bar.set_edgecolor(ec)
        bar.set_hatch(hatch)
        bar.set(fill=False)
        ax2.annotate(f"{bar.get_height():.0f}", (bar.get_x() + bar.get_width()/5,
                     bar.get_height() + ax2.get_ylim()[-1]*0.025), color=ec, fontsize="small")
    ax2.set_xlabel(f"(c) {name2}", fontsize="small")
    # ax2.ticklabel_format(axis='y', style='sci', scilimits=(4, 4), useMathText=True)
    ax2.set_ylim((0, 325))
    ax2.tick_params(axis='x', which='major', labelsize=6)

    fig.tight_layout(pad=1)
    fig.savefig('plot.pdf', dpi=1000, bbox_inches='tight')
    plt.cla()
