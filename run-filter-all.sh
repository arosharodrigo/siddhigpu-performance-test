#!/bin/bash

DIR="$( cd -P "$( dirname "$0" )" && pwd )"
cd "$DIR"

JVM=/home/arosha/install/java/jdk1.6.0_29/jre/bin/java
JAR=target/debs2013-1.0.0-SNAPSHOT-jar-with-dependencies.jar

DEBS_FILE=/home/arosha/projects/Siddhi/Data-Set/full-game

# a, enable-async             Enable Async processing
# g, enable-gpu               Enable GPU processing
# r, ringbuffer-size          Disruptor RingBuffer size - in power of two
# Z, batch-max-size           GPU Event batch max size
# z, batch-min-size           GPU Event batch min size
# t, threadpool-size          Executor service pool size
# b, events-per-tblock        Number of Events per thread block in GPU
# s, strict-batch-scheduling  Strict batch size policy
# w, work-size                Number of events processed by each GPU thread - 0=default
# i, input-file               Input events file path
# u, usecase                  Name of the usecase
# p, execplan                 Name of the ExecutionPlan
# c, usecase-count            Usecase count per ExecutionPlan
# x, execplan-count           ExecutionPlan count
# m, use-multidevice          Use multiple GPU devices
# d, device-count             GPU devices count
# l, selector-workers         Number of worker thread for selector processor - 0=default

a="false"
g="true"
r=8192
t=32
b=128
z=2048
Z=2048
s="false"
w=10
l=0

u="filter"
p="Filter"

#JOPT=-Djava.compiler=NONE
JOPT="-Xms2g -Xmx6g"
LOG=logs/${p}

if false; then

for x in 1; do
for c in 1 2 3 4 5 6 8 9 10 12 15 16 20 25; do

APP_CPU_ST="${JVM} ${JOPT} -jar ${JAR} --enable-async false --enable-gpu false --usecase ${u} --execplan ${p} --execplan-count ${x} --usecase-count ${c} --ringbuffer-size ${r} --threadpool-size ${t} --events-per-tblock 0 --batch-max-size 0 --batch-min-size 0 --strict-batch-scheduling false --work-size 0 --selector-workers 0 --use-multidevice false --device-count 0 --input-file ${DEBS_FILE}"

echo "Running >> ${APP_CPU_ST}"
${APP_CPU_ST} &>${LOG}_r${r}_x${x}_c${c}_cpu_st.log
cp logs/performance.log ${LOG}_r${r}_x${x}_c${c}_cpu_st_performance.log

done
done

exit;
fi

if true; then

#for z in 2048 4096 8192; do
for z in 2048; do
for x in 1 2 3 4 5 6 8 9 10 12 15 16 20 25; do
for c in 1; do

Z=$((z*1))
t=32

APP_CPU_MT="${JVM} ${JOPT} -jar ${JAR} --enable-async true --enable-gpu false --usecase ${u} --execplan ${p} --execplan-count ${x} --usecase-count ${c} --ringbuffer-size ${r} --threadpool-size ${t} --events-per-tblock ${b} --batch-max-size ${Z} --batch-min-size ${z} --strict-batch-scheduling ${s} --work-size ${w} --selector-workers ${l} --use-multidevice false --device-count 0 --input-file ${DEBS_FILE}"

APP_GPU_SD="${JVM} ${JOPT} -jar ${JAR} --enable-async true --enable-gpu true --usecase ${u} --execplan ${p} --execplan-count ${x} --usecase-count ${c} --ringbuffer-size ${r} --threadpool-size ${t} --events-per-tblock ${b} --batch-max-size ${Z} --batch-min-size ${z} --strict-batch-scheduling ${s} --work-size ${w} --selector-workers ${l} --use-multidevice false --device-count 0 --input-file ${DEBS_FILE}"

echo "Running >> ${APP_CPU_MT}"
${APP_CPU_MT} &>${LOG}_r${r}_z${z}_Z${Z}_b${b}_x${x}_c${c}_m${m}_cpu_mt.log
cp logs/performance.log ${LOG}_r${r}_z${z}_Z${Z}_b${b}_x${x}_c${c}_m${m}_cpu_mt_performance.log

echo "Running >> ${APP_GPU_SD}"
${APP_GPU_SD} &>${LOG}_r${r}_z${z}_Z${Z}_b${b}_x${x}_c${c}_sd_gpu.log
#nvprof --print-api-trace --print-gpu-trace --output-profile prof.out --log-file ${LOG}_r${r}_z${z}_Z${Z}_b${b}_x${x}_c${c}_sd_gpu_nvprof.log ${APP_GPU_SD} &>${LOG}_r${r}_z${z}_Z${Z}_b${b}_x${x}_c${c}_sd_gpu.log
cp logs/performance.log ${LOG}_r${r}_z${z}_Z${Z}_b${b}_x${x}_c${c}_sd_gpu_performance.log

done
done
done

exit;
fi

if false; then
x=1
c=1
z=2048
Z=2048
APP="${JVM} ${JOPT} -jar ${JAR} --enable-async true --enable-gpu true --usecase ${u} --execplan ${p} --execplan-count ${x} --usecase-count ${c} --ringbuffer-size ${r} --threadpool-size ${t} --events-per-tblock ${b} --batch-max-size ${Z} --batch-min-size ${z} --strict-batch-scheduling ${s} --work-size ${w} --selector-workers ${l} --use-multidevice false --device-count 0 --input-file ${DEBS_FILE}"
nvprof --print-api-trace --print-gpu-trace --output-profile logs/prof.out --log-file logs/prof.log ${APP}
exit;
fi

#cuda-memcheck --report-api-errors yes ${APP}
#cuda-memcheck --leak-check full ${APP} 
#cuda-memcheck --tool racecheck  ${APP}
#cuda-gdb ${JVM}
#valgrind -v --leak-check=full --show-reachable=yes --track-origins=yes --num-callers=20 --track-fds=yes --trace-children=yes --log-file=valg.log ${APP}
#valgrind -v --gen-suppressions=all --trace-children=yes --log-file=valg.log ${APP}

#nvprof --import-profile prof.out #- Summarized report will generate