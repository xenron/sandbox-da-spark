#!/bin/bash

MYFILE=$1

if [ $# -eq 2 ]
then
  SLIMIT=$2
fi

echo ""
echo "check $MYFILE"
echo ""


cat $MYFILE | while read line
do
  lineSize=`echo $line | tr "," "\n" | wc -l`

  if [ $# -eq 2 ]
  then
    if [  $lineSize -ne $SLIMIT ]
    then
      echo -n "Line records = "
      echo $lineSize
    fi
  else
    echo -n "Line records = "
    echo $lineSize
  fi
done
