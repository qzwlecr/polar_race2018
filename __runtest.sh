#!/bin/bash

make clean
make
cd test
make
./test
cd ..
