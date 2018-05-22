#!/bin/bash

OPTION=$1

if [ "#OPTION"=="crt" ]; then
if [ ! -d /home/input ]; then
    mkdir /home/input
else
    echo "folder already exists"
fi
elif [ "#OPTION"=="rm" ]; then
if [ ! -d /home/input ]; then
     echo "folder doesnt exists"
else
    rm -r /home/input
fi
fi