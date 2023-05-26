#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.legend_handler import HandlerTuple
import string
import openpyxl
from itertools import chain
import scienceplots

textwidth = 7
columnwidth = 3.34
# circle, triangle_up, square, pentagon, plus (filled), star, diamond
markers = ["^", "s", "p", "P", "*", "D"]
# colors = ['#474747D0', '#0C5DA5D0', '#845B97D0',
#           '#00B945D0', '#FF9500D0', '#FF2C00D0', '#9E9E9ED0']
colors = ['#0C5DA5D0', '#845B97D0',
          '#00B945D0', '#FF9500D0', '#9E9E9ED0', '#FF2C00D0', '#9E9E9ED0']
patterns = ['///', '\\\\', '++', 'XX', '\|', '-\\', '--']
plt.style.use(["default", "science", "nature", "../jlhu.mplstyle"])


if __name__ == "__main__":
    # read all sheets, one sheet for one subplot
    data = pd.read_excel("data.xlsx", sheet_name=None)
    fig, (lax, rax) = plt.subplots(1, 2, gridspec_kw={'width_ratios': [5, 3]})
    (lname, rname) = data.keys()
    fig.set_size_inches(columnwidth, columnwidth/2.3)

    sheet = data[lname]
    x = sheet.iloc[:, 0]  # the first column is the thread num
    competitors = sheet.columns.tolist()[1:]
    for j, design in enumerate(competitors):
        lax.plot(x, sheet[design], marker=markers[j], color=colors[j])
    # lax.legend(lax.get_lines(), competitors, bbox_to_anchor=(0.5, 1.1), loc='lower center', ncol=2, fontsize="small")
    # lax.legend(lax.get_lines(), competitors,
    #            loc='upper left', fontsize="small")
    lax.set_xlabel(f"Num. of Threads\n(a) {lname}")
    lax.ticklabel_format(axis='y', style='sci', scilimits=(
        6, 6), useMathText=True)  # set SCI format in Y axis
    lax.set_ylabel('Average Throughput (ops)', labelpad=1)

    sheet = data[rname]
    sheet.index = sheet.iloc[:, 0]
    colordict = dict(
        zip(sheet.columns[1:], colors))
    sheet.iloc[:, 1:5].plot(
        ax=rax, kind='bar', color=colordict, width=2., rot=0, legend=False, align='edge')
    bars = rax.patches
    edgecolor = [c for c in colors for _ in range(len(sheet))]
    hatches = [p for p in patterns for _ in range(len(sheet))]
    handles = []
    for i, (bar, hatch, ec) in enumerate(zip(bars, hatches, edgecolor)):
        bar.set_edgecolor(ec)
        bar.set_hatch(hatch)
        bar.set(fill=False)
        if i % len(sheet) == 0:
            handles.append(bar)
    # rax.legend(bbox_to_anchor=(0.5, 1.1), loc='lower center', ncol=2, fontsize="small")
    # rax.legend(loc='best', ncol=2)
    rax.set_xlabel(f"\n(b) {rname}")
    rax.set_ylabel('Total Bytes Written (B)', labelpad=1)
    rax.ticklabel_format(axis='y', style='sci',
                         scilimits=(6, 6), useMathText=True)
    xmin = rax.patches[0].get_x()-rax.patches[0].get_width()
    xmax = rax.patches[-1].get_x()+2*rax.patches[-1].get_width()
    y = sheet.iloc[0, 5]
    exp = rax.plot([xmin, xmax], [y, y], color=colors[-1], linestyle='--')
    # 报错为 None
    # rax.set_xticks(rax.get_xticks(), rotation=180)

    fig.legend(chain(zip(lax.get_lines(), handles), [exp[0]]), chain(competitors, ["Expected"]),
               handler_map={tuple: HandlerTuple(ndivide=None)}, bbox_to_anchor=(0.5, .95), loc='lower center', ncol=5,
               handletextpad=0.2, columnspacing=.8)
    fig.tight_layout(pad=0.5)
    fig.savefig('plot.pdf', dpi=1000, bbox_inches='tight')
    plt.cla()
