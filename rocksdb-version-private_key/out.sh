#!/bin/bash
cat 14e6.server.out | grep Trace-_3_4 > Fast-Trace-_3_4.server.out
cat 14e6.server.out | grep Trace-_2_4 > Fast-Trace-_2_4.server.out
cat 14e6.server.out | grep Trace-_1_4 > Fast-Trace-_1_4.server.out
cat 14e6.server.out | grep Trace-_0_4 > Fast-Trace-_0_4.server.out

cat 14e6.client.out | grep Trace-_0_4 > Fast-Trace-_0_4.client.out
cat 14e6.client.out | grep Trace-_1_4 > Fast-Trace-_1_4.client.out
cat 14e6.client.out | grep Trace-_2_4 > Fast-Trace-_2_4.client.out
cat 14e6.client.out | grep Trace-_3_4 > Fast-Trace-_3_4.client.out

