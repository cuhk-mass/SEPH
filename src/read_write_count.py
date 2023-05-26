import sys
import os
import time


class performance_process:
    def __init__(self, design=None):
        if design != None:
            self.input_file = f"./data/{design}.txt"
            self.output_file = f"./data/{design}.csv"
        elif len(sys.argv) == 1:
            self.input_file = "result.txt"
            self.output_file = "result.csv"
        elif len(sys.argv) == 2:
            self.input_file = f"./data/{sys.argv[1]}.txt"
            self.output_file = f"./data/{sys.argv[1]}.csv"
        else:
            self.input_file = sys.argv[1]
            self.output_file = sys.argv[2]

        self.title = "TITLE:"+str(time.asctime(time.localtime(time.time())))
        f = open(self.input_file, 'r')
        self.ss = f.readlines()
        self.Metrics = ['MediaReads', 'MediaWrites',
                        'ReadRequests', 'WriteRequests']
        self.DIMMs = ["DimmID=0x0001", "DimmID=0x0011", "DimmID=0x0021",
                      "DimmID=0x0101", "DimmID=0x0111", "DimmID=0x0121"]
        f.close()
        self.dictlist = [{}, {}]

    def build_dict(self):
        index = 0
        dimm = ''
        for s in self.ss:
            if '//' in s:
                continue
            if s.strip() == 'after':
                index += 1
                # print(index)
            elif '---' in s:
                dimm = s.strip().strip('-')
                self.dictlist[index][dimm] = {}
            elif '=' in s:
                ls = s.strip().split('=')
                self.dictlist[index][dimm][ls[0]] = ls[1]

    def output(self):
        t = open(self.output_file, 'w')
        for k in self.dictlist[0]:
            t.write(k+'\n')
            for k1 in self.dictlist[0][k]:
                t.write(
                    k1+','+str(eval(self.dictlist[0][k][k1]))+','+str(eval(self.dictlist[1][k][k1]))+'\n')
        t.close()

    def query(self, target):
        sum = 0
        for k in self.DIMMs:
            sum += eval(self.dictlist[1][k][target]) - \
                eval(self.dictlist[0][k][target])
        return sum*64/(1024**3)  # in GiB

    def RW_output(self):
        exist_flag = os.path.exists(self.output_file)
        t = open(self.output_file, 'a')
        if exist_flag:
            for target in self.Metrics:
                t.write(','+target+"(GiB)")
            t.write('\n')

        t.write(self.title)

        output_string = ""
        for target in self.Metrics:
            output_string += ',  '+str(a.query(target))
        t.write(output_string+'\n')
        t.close()

    def show_diff_dimm(self):
        t = open(self.output_file, 'w')
        t.write(self.title)
        t.write(',')
        for k in self.DIMMs:
            t.write(k+',')
        t.write('\n')
        for metrics in self.Metrics:
            t.write(metrics+',')
            for k in keys:
                t.write(
                    str(eval(self.dictlist[1][k][metrics])-eval(self.dictlist[0][k][metrics]))+',')
            t.write('\n')
        t.close()


for design in ['steph', 'level', 'cceh', 'dash', 'clevel', 'pclht']:
    a = performance_process(design)
    a.build_dict()
    a.RW_output()
