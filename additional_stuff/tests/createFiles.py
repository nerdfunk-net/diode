#!/usr/bin/python

import argparse
import os
import time
from time import sleep
from random import randint

def main():

    number = 0
    sum = 0

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", help="increase output verbosity", action="store_true")
    parser.add_argument("--number", type=int, help="The number of files created", default=60)
    parser.add_argument("--fpm", type=int, help="Number of Files per minute")
    parser.add_argument("--prefix", type=str, help="Prefix used as name", default="test")
    parser.add_argument("--directory", type=str, help="The destination Directory", required=True)
    parser.add_argument("--bandwidth", type=int, help="Bandwidth")   
    parser.add_argument("--min", type=int, help="The minimum size of the file", default=1000)
    parser.add_argument("--max", type=int, help="The maximum size of the file", default=10000)

    args = parser.parse_args()

    if args.verbose:
        print("verbosity turned on")
    if args.number:
        number = args.number
    if args.fpm:
        fpm = args.fpm
    if args.bandwidth:
        bandwidth = args.bandwidth
    if args.prefix:
        prefix = args.prefix
    if args.min:
        min = args.min
    if args.max:
        max = args.max
    if args.directory:
        directory = args.directory

    if number == 0:
        print ("Number must be > 0");
        os._exit(1)

    #
    # 1. example: fpm==60 bandwidth==100 (Mbit/s):
    #             sleepingtime=60/60 == 1
    #             min=max=(100/8 * 1) == 12.5 MByte
    # 2. example: fpm=120 bandwidth==100 (Mbit/s):
    #             sleepingime=60/120 == 0.5
    #             min=max=(100/8 * 0.5) == 6.25 MByte
    #

    current_epoch = time.time()

    sleepingtime = 60.0 / fpm
    if args.bandwidth:
        mbyte = bandwidth * 1000000 // 8
        min=max=int(mbyte * sleepingtime)

    print ("Sleeping time {:6.2f} fpm {:d} min {:d} max {:d}".format(sleepingtime, fpm, min, max))

    for x in range(0, number):
        size=value = randint(min, max)
        sum+=size
        filename="%s/%s_%d" % (directory,prefix,x)
        print ("%d: Writing %s (%d)" % (time.time(), filename, size))
        with open(filename, 'wb') as f:
            f.write(os.urandom(size))
            sleep(sleepingtime)

    print ("Running Time: %s Bytes: %d" % ( (time.time() - current_epoch), sum))

if __name__ == "__main__":
    main()