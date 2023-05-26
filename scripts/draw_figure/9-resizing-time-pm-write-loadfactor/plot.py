#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import string
import openpyxl
import scienceplots

textwidth = 7
columnwidth = 3.34
# circle, triangle_up, square, pentagon, plus (filled), star, diamond
markers = ["o", "^", "s", "p", "P", "*", "D"]
patterns = ['-', '//', '\\\\', 'XX', 'xx', '-\\', '\\|', '']
plt.style.use(["default", "science", "nature", "../jlhu.mplstyle"])
colors = plt.rcParams["axes.prop_cycle"].by_key()['color']

if __name__ == "__main__":
    # read all sheets, one sheet for one subplot
    data = pd.read_excel("data.xlsx", sheet_name=None, index_col=None)
    fig, (ax0, ax1, ax2) = plt.subplots(
        1, 3, gridspec_kw={'width_ratios': [7, 7, 7]})
    (name0, name1, name2) = data.keys()
    fig.set_size_inches(columnwidth, columnwidth/2.3)

    lcolors = colors[:6] + ['#FF2C00D0']
    sheet = data[name0]
    # sheet.iloc[0, :] /= 1e9
    sheet.transpose().plot(
        ax=ax0, kind='bar', color=lcolors, width=1., rot=45, legend=False, fontsize="small")
    # ax0.set_xticks(sheet.columns, rotation=15)
    bars = ax0.patches
    edgecolor = [c for c in lcolors for _ in range(len(sheet))]
    hatches = [p for p in patterns for _ in range(len(sheet))]
    for bar, hatch, ec in zip(bars, hatches, edgecolor):
        bar.set_edgecolor(ec)
        bar.set_hatch(hatch)
        bar.set(fill=False)
        ax0.annotate(f"{bar.get_height():.0f}", (bar.get_x(),
                     bar.get_height() + ax0.get_ylim()[-1]*0.0025), fontsize="small", color=ec)
    # ax0.legend(loc='best', ncol=2)
    # ax0.set_ylabel('Total Resizing Time (s)')
    ax0.ticklabel_format(axis='y', style='sci', scilimits=(
        2, 2), useMathText=True)  # set SCI format in Y axis
    ax0.set_ylim((0, 500))
    ax0.set_xlabel(f"(a) {name0}", fontsize="small")
    ax0.set_xticklabels([])

    sheet = data[name1]
    colordict = dict(
        zip(sheet.columns[:], colors))
    sheet.iloc[:, 0:7].transpose().plot(
        ax=ax1, kind='bar', color=colors, width=1., rot=45, legend=False, fontsize="small", use_index=False)
    bars = ax1.patches

    edgecolor = [c for c in colors for _ in range(len(sheet))]
    hatches = [p for p in patterns for _ in range(len(sheet))]
    for bar, hatch, ec in zip(bars, hatches, edgecolor):
        bar.set_edgecolor(ec)
        bar.set_hatch(hatch)
        bar.set(fill=False)
        ax1.annotate(f"{bar.get_height()/1e10:.0f}", (bar.get_x(),
                     bar.get_height() + ax1.get_ylim()[-1]*0.025), fontsize="small", color=ec)

    xmin = ax1.get_xticks()[0]-ax1.patches[0].get_width()
    xmax = ax1.get_xticks()[-1]+ax1.patches[-1].get_width()
    y = sheet.iloc[0, 7]
    exp = ax1.plot([xmin, xmax], [y, y], color=colors[7], linestyle='--')
    # ax1.annotate(f"expected", (ax1.get_xticks()[2], y*.95), fontsize="small", color=colors[0], bbox=dict(boxstyle='square', fc='#FFFFFFB0', ec='none'))
    ax1.set_xlabel(f"(b) {name1}", fontsize="small")
    # ax1.set_ylabel('Total Bytes Written (B)')
    ax1.set_ylim((0, 2.2*1e11))
    ax1.ticklabel_format(axis='y', style='sci',
                         scilimits=(10, 10), useMathText=True)
    ax1.set_xticklabels([])

    sheet = data[name2]
    sheet.transpose().plot(
        ax=ax2, kind='bar', color=colors, width=1., rot=45, legend=False, fontsize="small")
    # ax2.set_xticks(sheet.columns, rotation=15)
    bars = ax2.patches
    edgecolor = [c for c in colors for _ in range(len(sheet))]
    hatches = [p for p in patterns for _ in range(len(sheet))]
    for bar, hatch, ec in zip(bars, hatches, edgecolor):
        bar.set_edgecolor(ec)
        bar.set_hatch(hatch)
        bar.set(fill=False)
        ax2.annotate(f"{bar.get_height():.1f}", (bar.get_x(),
                     bar.get_height() + ax2.get_ylim()[-1]*0.025), fontsize="small", color=ec)
    # ax2.legend(loc='best', ncol=2)
    ax2.set_xlabel(f"(c) {name2}", fontsize="small")
    ax2.set_ylim((0, .8))
    ax2.set_xticklabels([])

    labels = data[name1].columns.tolist()[:]
    handles = [h for h in ax1.patches]
    handles.append(exp[0])

    fig.legend(handles, labels, ncol=len(labels),
               bbox_to_anchor=(0.5, .98), loc='lower center', handletextpad=0.2, columnspacing=.8, fontsize="x-small")
    # ax1.legend(handles[-1:], labels[-1:], bbox_to_anchor=(0.5, -.25), loc='lower center', ncol=2, handletextpad=0.2, columnspacing=.5, fontsize="small")

    fig.tight_layout(pad=0.1)
    fig.savefig('plot.pdf', dpi=1000, bbox_inches='tight')
    plt.cla()
