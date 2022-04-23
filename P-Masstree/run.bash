#!/usr/bin/env bash

for var in "$@"; do
  if [ "$var" = "build" ]; then

    # build ralloc
    cd /home/xiaoxiang/ralloc/test/ || exit
    git pull
    make clean
    make libralloc.a
    if [ ! -f libralloc.a ]; then
      echo "Failed to build ralloc!"
      exit
    fi

    # build P-Masstree
    cd /home/xiaoxiang/RECIPE/P-Masstree/ || exit
    git pull
    rm -rf build && mkdir build
    cd build || exit
    cmake .. && make -j
    if [ ! -f example ]; then
      echo "Failed to build P-Masstree!"
      exit
    fi

    echo "" && echo "############" && echo "OK" && echo "############" && echo ""
  fi
done

cd /home/xiaoxiang/RECIPE/P-Masstree/build/ || exit
rm -rf insert.csv lookup.csv

index_location=("dram" "pmem")
value_location=("dram" "pmem")
num_threads=(1 2 4 8 16)

# the header of csv file
printf "index,value," >>insert.csv
printf "index,value," >>lookup.csv
for n in "${num_threads[@]}"; do
  printf "$n," >>insert.csv
  printf "$n," >>lookup.csv
done
echo "" >>insert.csv
echo "" >>lookup.csv

for i in "${index_location[@]}"; do
  for v in "${value_location[@]}"; do

    # the first two columns
    printf "$i,$v," >>insert.csv
    printf "$i,$v," >>lookup.csv

    for n in "${num_threads[@]}"; do
      rm -rf /pmem0/*
      /home/blepers/linux/tools/perf/perf record ./example 100000000 "$n" index="$i" value="$v"
#      ./example 100 "$n" index="$i" value="$v"
    done

    # this should result in two csv files insert.csv and lookup.csv
    # just append a new line to it
    echo "" >>insert.csv
    echo "" >>lookup.csv

  done
done
