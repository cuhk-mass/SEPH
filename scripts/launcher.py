import os
import sys
from tabnanny import check

from scripts.config import *


class launcher:
    def __init__(self, command, target_config) -> None:
        # directory check
        if not os.path.exists('data'):
            os.makedirs('data')
        if not os.path.exists('data/tmp'):
            os.makedirs('data/tmp')
        if not os.path.exists('rawdata'):
            os.makedirs('rawdata')

        # command check
        if (command not in ['launch', 'build', 'launch_all', 'check', 'complete']):
            print(
                "Please run: python3 run.py [launch|build|launch_all] [config]")
            return
        # config check
        if command in ['launch', 'build']:
            try:
                eval(target_config)
            except:
                print(f"target config '{target_config}' does not exist")
                return

        self.testees = ['steph', 'level', 'cceh',
                        'cceh_cow', 'dash', 'clevel', 'pclht']

        # command process
        if command == 'check':
            self.check_data_integrity()
        elif command == 'complete':
            # complete with naive log file (all zero) to test scripts
            self.compelete_data_file()
        elif command == 'launch_all':
            self.launch_all()
        elif command == 'launch':
            self.launch(target_config)
        elif command == 'build':
            self.build(target_config)
        else:
            print("not support")

    def build(self, target_config):
        target_config = eval(target_config)
        cmd = f'meson builddir --reconfig --buildtype release '\
            f'-DPMHB_LATENCY={target_config["LATENCY"]} -DINSERT_DEBUG=false -DCOUNTING_WRITE={target_config["COUNTING_WRITE"]} '\
            f'-DLOAD_FACTOR={target_config["LOAD_FACTOR"]} -DPREFAULT=true '\
            f'-DBREAKDOWN_SOD={target_config["BREAKDOWN_SOD"]} -DBREAKDOWN_SO={target_config["BREAKDOWN_SO"]} '\
            f'-DBREAKDOWN_S={target_config["BREAKDOWN_S"]} -DBREAKDOWN_BASE={target_config["BREAKDOWN_BASE"]} >> compile.log && '\
            f'meson compile -C builddir -j 20 >> compile.log'
        print(cmd)
        os.system(cmd)

    def launch(self, target_config):
        self.build(target_config)

        # output directory checker

        data_path = f'./data/{target_config}'
        target_config = eval(target_config)
        if not os.path.exists(data_path):
            os.mkdir(data_path)

        # generate commands
        cmds = []
        for th_num in target_config['thread_num']:
            testees = self.testees
            if 'breakdown' in data_path:
                testees = ['steph']
            for testee in testees:
                tmp_th_num = th_num
                if testee in ['steph', 'clevel'] and th_num == 48:
                    tmp_th_num = 47
                cmd = f'numactl -N0 ./builddir/pmhb -w /mnt/pmem0/Testee/ -o {data_path} '\
                    f'-l /home/cwang/load/{target_config["workload"]}.ycsb '\
                    f'-r /home/cwang/run/{target_config["workload"]}.ycsb '\
                    f'-m /mnt/pmem0/Trace/{target_config["workload"]} '\
                    f'--load_num {target_config["load_num"]} --run_num {target_config["run_num"]} '\
                    f'-e {testee} -t {tmp_th_num} '\
                    f'> {data_path}/{testee}_{tmp_th_num}.txt'
                cmds.append(cmd)
        # run commands
        for cmd in cmds:
            # three tries because clevel can cause segmentation fault
            print(cmd)
            for i in range(3):
                if os.system(cmd) == 0:
                    break
                else:
                    print("[Retry]", cmd)
            else:
                # fail three times, and we will put zero at all results.
                print("[FAIL!]", cmd)
                os.system("cp naivelog.txt " + cmd.split()[-1])

    def launch_all(self):

        pm_files = []
        for target_config in all_configurations:
            target_config = eval(target_config)
            pm_files.append('/mnt/pmem0/Trace/'+target_config['workload'])

        for file in pm_files:
            if not os.path.exists(file):
                print(
                    f"[Error] pm trace file {file} is missing. Please run trace_maker first")
                exit(0)

        for target_config in all_configurations:
            self.launch(target_config)

    def get_all_data_filename(self):
        result = []
        for target_config in all_configurations:
            data_path = f'./data/{target_config}'
            target_config = eval(target_config)

            for th_num in target_config['thread_num']:
                testees = self.testees
                if 'breakdown' in data_path:
                    testees = ['steph']
                for testee in testees:
                    tmp_th_num = th_num
                    if testee in ['steph', 'clevel'] and th_num == 48:
                        tmp_th_num = 47
                    result.append(f'{data_path}/{testee}_{tmp_th_num}.txt')

        return result

    def check_data_integrity(self, v=True):
        # 每个文件都应该具有 "Output over" 字样
        self.incomplete_files = []
        self.missing_files = []
        filenames = self.get_all_data_filename()
        for file in filenames:
            if not os.path.exists(file):
                self.missing_files.append(file)
            else:
                s = '\n'.join(open(file, 'r').readlines())
                if s.find("Output over") == -1:
                    self.incomplete_files.append(file)
        if v:
            if (len(self.incomplete_files) + len(self.missing_files) == 0):
                print("Data files are complete for plotting")
            else:
                print("incomplete data (which means the run failed):")
                print(self.incomplete_files)

                print("missing (which means the run didn't start)")
                print(self.missing_files)
        return (self.incomplete_files, self.missing_files)

    def compelete_data_file(self):
        self.check_data_integrity(False)
        for filename in self.incomplete_files+self.missing_files:
            if not os.path.exists(os.path.dirname(filename)):
                os.mkdir(os.path.dirname(filename))
            os.system("cp ./naivelog.txt " + filename)
