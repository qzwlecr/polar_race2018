#!/bin/bash
echo Building engine library (libengine.a)
make 2>&1
cd test
echo Building test program (linking)
make 2>&1
