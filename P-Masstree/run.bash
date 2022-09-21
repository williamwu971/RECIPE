#!/usr/bin/env bash

for var in "$@"; do
  if [ "$var" = "build" ]; then

    #    apt-get update
    #    apt-get install -y build-essential cmake libboost-all-dev libpapi-dev default-jdk
    #    apt-get install -y libtbb-dev libjemalloc-dev libpmem-dev

    # build P-Masstree
    cd /mnt/sdb/xiaoxiang/RECIPE/P-Masstree/ || exit
    git pull || exit
    #    rm -rf build && mkdir build
    rm -rf build/*
    cd build || exit
    cmake .. && make -j
    if [ ! -f example ]; then
      echo "Failed to build P-Masstree!"
      exit
    fi

    echo "" && echo "############" && echo "masstree build OK" && echo "############" && echo ""
    exit
  fi

  if [ "$var" = "ralloc" ]; then
    # build ralloc
    cd /home/xiaoxiang/ralloc/test/ || exit
    git pull
    make clean
    make -j libralloc.a
    if [ ! -f libralloc.a ]; then
      echo "Failed to build ralloc!"
      exit
    fi
    echo "" && echo "############" && echo "ralloc build OK" && echo "############" && echo ""
    exit
  fi

  if [ "$var" = "pmdk" ]; then
    # build ralloc
    cd /mnt/sdb/xiaoxiang/pmdk/ || exit
    git pull
    make -j || exit
    make -j install || exit
    echo "" && echo "############" && echo "pmdk build OK" && echo "############" && echo ""
    exit
  fi

done

cd build || exit
rm -f ./*.latencies ./*.png ./*.csv

#pmdk_no_flush=("0" "1")
pmdk_no_flush=("0")
index_location=("dram" "ralloc" "obj")
value_location=("ralloc" "log" "obj")
index_location=("dram")
#value_location=("ralloc")
#value_location=("log")
value_location=("obj")
#value_location=("log" "obj")
#num_threads=(1 3 5 7 9 11 13 15)
num_threads=(16)
#num_threads=(1)
use_perf="yes"
record_latency="yes"
num_of_gc=(8 0)
num_of_gc=(8)

workload=16000000
workload=1600000
key_order="random"
#key_order="seq"
value_size=1024 # the size of the value impact performance a lot

file_prefixes=("perf")

for fp in "${file_prefixes[@]}"; do
  echo "$fp,workload=$workload,value_size=$value_size,key_order=$key_order" >"$fp".csv

  # the header of csv file
  {
    printf "index,value,threads,gc,pmdk_no_flush,"
    printf "insert_r(gb),insert_rb(gb/s),insert_w(gb),insert_wb(gb/s),insert_TP(ops/us),insert_gc_TP(ops/us),"
    printf "update_r(gb),update_rb(gb/s),update_w(gb),update_wb(gb/s),update_TP(ops/us),update_gc_TP(ops/us),"
    printf "lookup_r(gb),lookup_rb(gb/s),lookup_w(gb),lookup_wb(gb/s),lookup_TP(ops/us),lookup_gc_TP(ops/us),"
    printf "delete_r(gb),delete_rb(gb/s),delete_w(gb),delete_wb(gb/s),delete_TP(ops/us),delete_gc_TP(ops/us),"
  } >>"$fp".csv

  #  {
  #    printf "index,value,threads,gc,pmdk_no_flush,"
  #    printf "load_rb(gb/s),load_wb(gb/s),load_TP(ops/us),"
  #    printf "run_rb(gb/s),run_wb(gb/s),run_TP(ops/us),"
  #  } >>"$fp".csv

  #  {
  #    printf "index,value,threads,gc,pmdk_no_flush,"
  #    printf "insert_TP(ops/us),"
  #    printf "update_TP(ops/us),"
  #    printf "get_TP(ops/us),"
  #    printf "delete_TP(ops/us),"
  #  } >>"$fp".csv

  #  for n in "${num_threads[@]}"; do
  #    printf 'T=%s,' "$n" >>$fp.csv
  #  done

  echo "" >>"$fp".csv
done

for i in "${index_location[@]}"; do
  for v in "${value_location[@]}"; do
    for n in "${num_threads[@]}"; do
      for g in "${num_of_gc[@]}"; do
        for f in "${pmdk_no_flush[@]}"; do

          # backup perf files
          #        cd .. || exit
          #          for pfn in *.perf; do
          #            [ -f "$pfn" ] || break
          #            echo "backing up $pfn"
          #            mv "$pfn" "$pfn".old
          #          done
          #        cd - || exit

          # the first three columns
          printf '%s,%s,%s,%s,%s,' "$i" "$v" "$n" "$g" "$f" >>perf.csv

          # drop system cache and clear pmem device
          echo 1 >/proc/sys/vm/drop_caches
          rm -rf /pmem0/masstree*
          killall -w perf >/dev/null 2>&1
          #      /home/blepers/linux/tools/perf/perf record -g ./example "$workload" "$n" index="$i" value="$v" key="$key_order"
          PMEM_NO_FLUSH="$f" ./example "$workload" "$n" value_size="$value_size" index="$i" value="$v" key="$key_order" perf="$use_perf" gc="$g" latency="$record_latency" prefix="$i"-"$v"-"$n"-"$g"-NF"$f"

          if [ "$record_latency" = "yes" ]; then
            for filename in *.latencies; do
              #              python3 ../simple_graph.py --r "$filename" --fn graph-"$i"-"$v"-"$n"-"$g"-NF"$f"-"$filename" --ylim 100000000 --xlim "$workload" || exit
              python3 ../simple_graph.py --r "$filename" --fn graph-"$i"-"$v"-"$n"-"$g"-NF"$f"-"$filename" --y ops/ms --x time --ylim 1000 || exit
#              python3 ../simple_graph.py --r "$filename" --fn graph-"$i"-"$v"-"$n"-"$g"-NF"$f"-"$filename"|| exit
            done
          fi
          #      mv out.png out_"$i"_"$v".png
          #      ./example 100 "$n" index="$i" value="$v"

          # this should result in two csv files insert.csv and lookup.csv
          # just append a new line to it
          for fp in "${file_prefixes[@]}"; do
            echo "" >>"$fp".csv
          done
        done
      done
    done
  done
done

# move perf files
#for pfn in *.perf; do
#  [ -f "$pfn" ] || break
#  mv "$pfn" ../
#done
