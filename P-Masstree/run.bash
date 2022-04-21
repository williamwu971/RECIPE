#!/usr/bin/env bash

for var in "$@"; do
  if [ "$var" = "build" ]; then
    rm -rf build && mkdir build
    cd build
    cmake .. && make -j
    cd ..
    echo "" && echo "############" && echo "rebuilt" && echo "############" && echo ""
  fi
done

/home/blepers/linux/tools/perf/perf record ./build/example "$1" "$2"
