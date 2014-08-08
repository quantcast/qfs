#!/bin/bash

for w in 4 8 16 32 64 128 ; do
    ./gf_methods $w -A -U | sh -e
    if [ $? != "0" ] ; then
      echo "Failed unit tests for w=$w"
      break
    fi
done
