#!/usr/bin/env bash

if [ -d /home/input ]; then
    for f in /home/input/*
    do
    hadoop distcp file:${f} hdfs:/input/
    done
    rm -r /home/input
else
    echo "no input present"
fi