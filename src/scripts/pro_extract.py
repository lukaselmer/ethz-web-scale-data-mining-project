#!/usr/bin/env python2.7

import sys
import numpy as np

d = [[0 for j in range(2)] for i in range(10)]

if __name__ == '__main__':
    n = np.array(100)
    for line in sys.stdin:
        line = line.strip()
        num, _ = np.fromstring(line, sep='\t')
        for i in range(len(line)):
            if line[i] == '{':
                k = i
                break
        line2 = line[k+1:-1]
        content = line2.split(',')
        count = 0
        for x in content:
            pro1, pro2 = np.fromstring(x, sep=':')
            d[count][0] = pro1
            d[count][1] = pro2
            count += 1
        for x in range(10):
            if d[x][0] in n:
                continue
            n = np.append(n,d[x][0])
    for x in n:
        print x



