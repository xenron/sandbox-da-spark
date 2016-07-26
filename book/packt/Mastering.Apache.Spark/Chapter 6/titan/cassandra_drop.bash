#!/bin/bash

# This script drops the cassandra keyspace

echo ""
echo "Dropping key space titan"
echo ""

cqlsh <<EOF
drop keyspace titan;
quit;
EOF
