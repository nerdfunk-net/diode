#!/bin/bash

while getopts d:n:m: flag
do
    case "${flag}" in
        d) destination=${OPTARG};;
        n) number=${OPTARG};;
        m) min=${OPTARG};;
    esac
done

echo "Destinstion: $destination";
echo "Number of Files: $number";
echo "Min size: $min";

for (( n=0; n<$number; n++ ))
do
    dd if=/dev/urandom of=/$destination/file$( printf %03d "$n" ).bin bs=1 count=$(( RANDOM + $min ))
done

for file in $destination/*; do
    if [[ -f "$file" ]]; then
        md5 -- "$file" > "${file}.md5"
    fi
done