#!/bin/bash

# This script drops the hbase table

hbase shell <<EOF
disable 'titan'
drop 'titan'
exit
EOF
