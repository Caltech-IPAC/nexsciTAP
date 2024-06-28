#!/bin/bash

input=$1

mkdir -p results

while IFS= read -r line
do
  name=$(echo $line | cut -d':' -f 1)
  adql=$(echo $line | cut -d':' -f 2-)

  mkdir -p results/$name

  echo ""
  echo ""
  echo "---------------------------------------------------------------"
  echo $name
  echo ""
  echo $adql > results/$name/query.txt
  (cd results/$name && ../../run_query.bash "$line")

done < "$input"
