#!/bin/bash

#cat 14e6.server.out | grep Trace-_3_5 > Fast-Trace-_3_5.server.out
cat 14e6.server.out | grep Trace-_2_5 > Fast-Trace-_2_5.server.out
cat 14e6.server.out | grep Trace-_1_5 > Fast-Trace-_1_5.server.out
cat 14e6.server.out | grep Trace-_0_5 > Fast-Trace-_0_5.server.out

cat 14e6.client.out | grep Trace-_0_5 > Fast-Trace-_0_5.client.out
cat 14e6.client.out | grep Trace-_1_5 > Fast-Trace-_1_5.client.out
cat 14e6.client.out | grep Trace-_2_5 > Fast-Trace-_2_5.client.out
#cat 14e6.client.out | grep Trace-_3_5 > Fast-Trace-_3_5.client.out

