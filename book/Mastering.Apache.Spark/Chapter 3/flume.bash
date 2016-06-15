#!/bin/bash

# run the bash agent

flume-ng agent \
  --conf /etc/flume-ng/conf \
  --conf-file ./agent1.flume2.cfg \
  -Dflume.root.logger=DEBUG,INFO,console  \
  -name agent1

