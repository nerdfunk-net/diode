#!/bin/bash

while getopts d: flag
do
    case "${flag}" in
        d) directory=${OPTARG};;
    esac
done

for file in $directory/*.bin; do
  md5=$(md5 -q  $file)
  filename=${file}.md5
  orig=$(cat $filename | awk '{print $4}')
  if [[ "$md5" == "$orig" ]]
  then
    echo match
  else
    echo error
  fi
done