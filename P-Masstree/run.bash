#!/usr/bin/env bash

PREFIX="/mnt/sda/xiaoxiang"

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
    cd $PREFIX/RECIPE/P-Masstree/ || exit
    git pull
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

cd $PREFIX/RECIPE/P-Masstree/build/ || exit

index_location=("dram" "pmem")
value_location=("pmem" "log")
index_location=("dram")
value_location=("log")
num_threads=(16)

workload=100000000
key_order="random"
#key_order="seq"

echo "insert,workload=$workload,unit=ops/us,key_order=$key_order" >insert.csv
echo "lookup,workload=$workload,unit=ops/us,key_order=$key_order" >lookup.csv

# the header of csv file
printf "index,value," >>insert.csv
printf "index,value," >>lookup.csv
for n in "${num_threads[@]}"; do
  printf 'T=%s,' "$n" >>insert.csv
  printf 'T=%s,' "$n" >>lookup.csv
done
echo "" >>insert.csv
echo "" >>lookup.csv

for i in "${index_location[@]}"; do
  for v in "${value_location[@]}"; do

    # the first two columns
    printf '%s,%s,' "$i" "$v" >>insert.csv
    printf '%s,%s,' "$i" "$v" >>lookup.csv

    for n in "${num_threads[@]}"; do
      rm -rf /pmem0/*
      echo "$workload" "$n" index="$i" value="$v" key="$key_order"
#      /home/blepers/linux/tools/perf/perf record -g ./example "$workload" "$n" index="$i" value="$v" key="$key_order"
      ./example "$workload" "$n" index="$i" value="$v" key="$key_order"
      #      ./example 100 "$n" index="$i" value="$v"
    done

    # this should result in two csv files insert.csv and lookup.csv
    # just append a new line to it
    echo "" >>insert.csv
    echo "" >>lookup.csv

  done
done
