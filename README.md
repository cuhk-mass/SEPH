# SEPH: Scalable, Efficient, and Predictable Hashing on Persistent Memory [OSDI'23]
**Directory Description**
- data: raw experiment results
- include: the benchmark implementation (pmhb)
- scripts: scritps to run experiments and plot figures
- src: nothing except main.cpp
- subprojects: implementation of all hash tables
- scripts/YCSB_TRACE: to generate the YCSB traces
- scripts/PM_TRACE: to preload YCSB traces into PM


## Instructions

In the second stage, we run the whole process of experiments, including generating workload, launching all tests and plotting all figures.

1. Clean the workloads

```bash
rm /mnt/pmem0/Trace/*
rm /mnt/pmem0/Testee/*
rm -rf ~/Trace_AE
```

2. Generate the YCSB traces (2 hours).

```bash
git clone https://github.com/brianfrankcooper/YCSB.git scripts/YCSB_TRACE/
cd scripts/YCSB_TRACE/
python3 run.py
# and wait for 2 hours (it usually 70 mins)
```

> To check if this step is over, you can run `top` command and see if there is a task named `java` with 100% CPU usage on the top of the list. If no `java` with 100% CPU usage, you can move to the next step safely.

3. Preload the YCSB trace to PM (30 mins).

```bash
cd scripts/PM_TRACE/
python3 parallel_run.py
# and wait for 1 hours (it usually takes 30 mins)
```

> To check if this step is over, you can run `ps -e | grep trace_maker`. If no ouput, you can move forward safely.

4. Run experiments to test hash index tables (24 hours, tmux recommended). 

```bash
# in the top directory
meson builddir --buildtype release -DPMHB_LATENCY=false -DINSERT_DEBUG=false -DCOUNTING_WRITE=false -DLOAD_FACTOR=false -DPREFAULT=true -DBREAKDOWN_SOD=true -DBREAKDOWN_SO=false -DBREAKDOWN_S=false -DBREAKDOWN_BASE=false > compile.log && meson compile -C builddir -j 20 >> compile.log

sudo python3 run.py launch_all > launch_all_ae.log
```

5. Plot figures and check the results

```bash
cd scripts/draw_figure
./auto_plot.sh
# check the figures (and all data.xlsx)
```




