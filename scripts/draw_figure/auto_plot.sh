#!/bin/bash
set -e
# 遍历当前目录下的所有目录
for dir in $(find . -type d); do

    # 判断该目录是否存在 fetch.py
    if [ -f "${dir}/fetch.py" ]; then 
        # 进入该目录并运行 fetch.py 和 plot.py 
        cd ${dir} 

        echo fetching for ${dir}
        python3 fetch.py 
        
        echo ploting for ${dir}
        python3 plot.py 
        
        cd .. 
    fi 

done