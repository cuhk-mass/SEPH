#!/bin/bash
set -e
# 遍历当前目录下的所有目录
for dir in $(find . -type d); do

    # 判断该目录是否存在 fetch.py
    if [ -f "${dir}/fetch.py" ]; then 
        # 进入该目录并运行 fetch.py 和 plot.py 
        cd ${dir} 

        if [ -f "data.xlsx" ]; then
            rm data.xlsx
        fi

        if [ -f "data_ref.xlsx" ]; then
            rm data_ref.xlsx
        fi

        if [ -f "plot_ref.pdf" ]; then
            rm plot_ref.pdf
        fi

        if [ -f "plot.pdf" ]; then
            rm plot.pdf
        fi
        
        cd .. 
    fi 

done