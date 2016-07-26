#!/bin/bash

index=0
limit=785
rowelem=0
rowsize=10

while [ $index -lt $limit ]
do
  echo -n "p($index),"

  let "rowelem=rowelem+1"

  if [ $rowelem -eq $rowsize ]
  then 
    echo ""
    rowelem=0
  fi

  let "index=index+1"
done
echo ""
echo ""
