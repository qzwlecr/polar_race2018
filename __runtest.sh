#!/bin/bash

make clean
make
cd test
make clean
make
./test
cd ..
