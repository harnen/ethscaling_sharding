#!/usr/bin/python3
import random
import sys


if(len(sys.argv) != 2):
    print("Provide the number of shard")
    exit(0)

shards = int(sys.argv[1])


print("from,to")
for i in range(0, 12000):
    print(random.randint(0, shards-1), ",", random.randint(0, shards-1), sep="")
