#!/bin/bash

#cat 14e6.server.out | grep Trace-_3_5 > Fast-Trace-_3_5.server.out
cat 14e6.server.out | grep -a Trace-_2_5 > 1-Fast-Trace-_2_5.server.out
cat 14e6.server.out | grep -a Trace-_1_5 > 1-Fast-Trace-_1_5.server.out
cat 14e6.server.out | grep -a Trace-_0_5 > 1-Fast-Trace-_0_5.server.out

cat 14e6.client.out | grep -a Trace-_0_5 > 1-Fast-Trace-_0_5.client.out
cat 14e6.client.out | grep -a Trace-_1_5 > 1-Fast-Trace-_1_5.client.out
cat 14e6.client.out | grep -a Trace-_2_5 > 1-Fast-Trace-_2_5.client.out
#cat 14e6.client.out | grep Trace-_3_5 > Fast-Trace-_3_5.client.out

