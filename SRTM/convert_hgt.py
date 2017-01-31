from __future__ import division, absolute_import, print_function
import os
import csv
import re
import sys
import numpy as np
import multiprocessing

re_split = re.compile("([NS])(\d+)([EW])(\d+)")

SAMPLES = 1201


def read_hgt_file(f):
    fname = os.path.split(f)[-1][:-4].upper()
    with open(f, "rb") as hgt_data:
        m = re_split.match(fname)
        if os.path.exists(fname + ".csv"):
            print(fname + " SKIP")
            return
        elif m is None:
            print(fname + " BAD MATCH")
            return
        print(fname)
        g = m.groups()
        base_lat = int(g[1])
        base_lon = int(g[3])

        if g[0] == "N":
            lat_sign = 1
        else:
            lat_sign = -1

        if g[2] == "E":
            lon_sign = 1
        else:
            lon_sign = -1

        try:
            elevations = np.fromfile(
                hgt_data,
                np.dtype('>i2'),
                SAMPLES*SAMPLES
            ).reshape((SAMPLES, SAMPLES))

            with open(fname + ".csv", "w") as outf:
                cw = csv.writer(outf)
                for x in range(0, SAMPLES):
                    for y in range(0, SAMPLES):
                        lat = lat_sign * (base_lat + (1200-y)/1200)
                        lon = lon_sign * (base_lon + x/1200)

                        hgt = elevations[y, x].astype(int)

                        cw.writerow([lat - 1/2400, lat + 1/2400, lon - 1/2400, lon + 1/2400, hgt])
        except Exception:
            print(fname + "FAIL")


def main():
    p = multiprocessing.Pool()
    for root, dirs, files in os.walk("dem3"):
        p.map(read_hgt_file, [root + "/" + f for f in files])


if __name__ == '__main__':
    main()
