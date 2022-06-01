#!/usr/bin/env bash

for var in "$@"; do
  if [ "$var" = "build" ]; then

    #    apt-get update
    #    apt-get install -y build-essential cmake libboost-all-dev libpapi-dev default-jdk
    #    apt-get install -y libtbb-dev libjemalloc-dev libpmem-dev

    # build ralloc
    #    cd $PREFIX/ralloc/test/ || exit
    #    git pull
    #    make clean
    #    make libralloc.a
    #    if [ ! -f libralloc.a ]; then
    #      echo "Failed to build ralloc!"
    #      exit
    #    fi

    # build P-Masstree
    git pull || exit
    rm -rf build && mkdir build
    cd build || exit
    cmake .. && make -j
    if [ ! -f example ]; then
      echo "Failed to build P-Masstree!"
      exit
    fi

    echo "" && echo "############" && echo "OK" && echo "############" && echo ""
    exit
  fi
done

# backup perf files
for pfn in *.perf; do
  [ -f "$pfn" ] || break
  cp "$pfn" "$pfn".old
done

cd build || exit

index_location=("dram" "pmem")
value_location=("pmem" "log")
index_location=("dram")
value_location=("log")
#value_location=("pmem")
num_threads=(1 3 5 7 9 11 13 15 17 19)
use_perf="yes"
record_latency="no"
num_of_gc=0

workload=20000000
key_order="random"
#key_order="seq"
value_size=1024

file_prefixes=("insert" "update" "lookup")

for fp in "${file_prefixes[@]}"; do
  echo "$fp,workload=$workload,unit=ops/us,key_order=$key_order" >$fp.csv

  # the header of csv file
  printf "index,value," >>$fp.csv

  for n in "${num_threads[@]}"; do
    printf 'T=%s,' "$n" >>$fp.csv
  done

  echo "" >>$fp.csv
done

rm -f latency.csv out.png

for i in "${index_location[@]}"; do
  for v in "${value_location[@]}"; do

    # the first two columns
    printf '%s,%s,' "$i" "$v" >>insert.csv
    printf '%s,%s,' "$i" "$v" >>lookup.csv

    for n in "${num_threads[@]}"; do

      # drop system cache and clear pmem device
      echo 1 >/proc/sys/vm/drop_caches
      rm -rf /pmem0/masstree*
      #      /home/blepers/linux/tools/perf/perf record -g ./example "$workload" "$n" index="$i" value="$v" key="$key_order"
      ./example "$workload" "$n" index="$i" value="$v" key="$key_order" perf="$use_perf" \
        gc="$num_of_gc" latency="$record_latency" value_size="$value_size"

      if [ "$record_latency" = "yes" ]; then
        python3 ../graph.py --r latency.csv --ylim 1000000
      fi
      #      mv out.png out_"$i"_"$v".png
      #      ./example 100 "$n" index="$i" value="$v"
    done

    # this should result in two csv files insert.csv and lookup.csv
    # just append a new line to it

    for fp in "${file_prefixes[@]}"; do
      echo "" >>$fp.csv
    done

  done

done

# move perf files
for pfn in *.perf; do
  [ -f "$pfn" ] || break
  mv "$pfn" ../
done
