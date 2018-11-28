#!/bin/bash

make clean
make
cd test
make clean
make
./test_with_kill
make clean
cd ..
make clean
