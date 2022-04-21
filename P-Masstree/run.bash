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

./build/example 10000 4
