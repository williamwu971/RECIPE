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

cd build || exit

index_location=("dram" "pmem" "obj")
value_location=("pmem" "log")
index_location=("dram")
value_location=("log")
#value_location=("pmem")
value_location=("obj")
#num_threads=(1 3 5 7 9 11 13 15)
num_threads=(17)
use_perf="yes"
record_latency="no"
num_of_gc=(8)

workload=20000000
key_order="random"
#key_order="seq"
value_size=1024 # the size of the value impact performance a lot

file_prefixes=("perf")

for fp in "${file_prefixes[@]}"; do
  echo "$fp,workload=$workload,value_size=$value_size,key_order=$key_order" >$fp.csv

  # the header of csv file
  printf "index,value,threads,gc," >>$fp.csv
  printf "insert_rb(gb/s),insert_wb(gb/s),insert_TP(ops/us)," >>$fp.csv
  printf "update_rb(gb/s),update_wb(gb/s),update_TP(ops/us)," >>$fp.csv
  printf "get_rb(gb/s),get_wb(gb/s),get_TP(ops/us)," >>$fp.csv
  printf "delete_rb(gb/s),delete_wb(gb/s),delete_TP(ops/us)," >>$fp.csv

  #  for n in "${num_threads[@]}"; do
  #    printf 'T=%s,' "$n" >>$fp.csv
  #  done

  echo "" >>$fp.csv
done

rm -f latency.csv out.png

for i in "${index_location[@]}"; do
  for v in "${value_location[@]}"; do
    for n in "${num_threads[@]}"; do
      for g in "${num_of_gc[@]}"; do

        # backup perf files
        for pfn in *.perf; do
          [ -f "$pfn" ] || break
          cp "$pfn" "$pfn".old
        done

        # the first three columns
        printf '%s,%s,%s,%s' "$i" "$v" "$n" "$g" >>perf.csv

        # drop system cache and clear pmem device
        echo 1 >/proc/sys/vm/drop_caches
        rm -rf /pmem0/masstree*
        #      /home/blepers/linux/tools/perf/perf record -g ./example "$workload" "$n" index="$i" value="$v" key="$key_order"
        ./example "$workload" "$n" index="$i" value="$v" key="$key_order" perf="$use_perf" \
          gc="$g" latency="$record_latency" value_size="$value_size"

        if [ "$record_latency" = "yes" ]; then
          python3 ../graph.py --r latency.csv --ylim 1000000
        fi
        #      mv out.png out_"$i"_"$v".png
        #      ./example 100 "$n" index="$i" value="$v"

        # this should result in two csv files insert.csv and lookup.csv
        # just append a new line to it
        for fp in "${file_prefixes[@]}"; do
          echo "" >>$fp.csv
        done
      done
    done
  done
done

# move perf files
for pfn in *.perf; do
  [ -f "$pfn" ] || break
  mv "$pfn" ../
done
