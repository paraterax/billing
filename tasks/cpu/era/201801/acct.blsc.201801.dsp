
SCCAS accounting information about jobs that are:
  - submitted by users blsc
  - accounted on all projects
  - completed between        Mon Jan  1 00:00:00 2018
                  end        Wed Jan 31 23:59:59 2018
-----------------------------------------------------------------------------------------

SUMMARY:      ( time unit: second )
Total number of completed jobs: 8            Total walltime consumed    : 2244920 
Total processors consumed     : 188          Total number of hosts used : 9       
Total wait time in queue      : 26595        Average wait time in queue : 3324.4      
-----------------------------------------------------------------------------------------

SCALE SUMMARY:       ( walltime unit: hour )
QueueName              <2       <4       <8      <16      <32      <64     <128     <256     <512    <1024    <2048    <4096    <8192   <16384

cpuII                 0.0      0.0      0.0      0.0    144.5      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0
c_blsc                0.0      0.0      0.0      0.0    479.1      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0
-----------------------------------------------------------------------------------------

QUEUES SUMMARY:      ( time unit: hour and second )
QueueName           Jobs    CPUs   Hosts  WallTime(h)  HostTime(h)  AverageScale  AvgWaitTime(sec)

cpuII                  6     144       6        144.5          6.0          24.0            4423.7
c_blsc                 2      44       3        479.1         24.1          22.0              26.5
-----------------------------------------------------------------------------------------

Completed jobs list: ( time unit: second )
JobID     GIndex   numProc   numHosts   WallTime(sec)   HostTime(sec)   ExitCode         QueueName               StartTime                 EndTime

6867193   0             24          1              72               3      32512             cpuII     2018/01/03 11:34:55     2018/01/03 11:34:58
6867916   0             24          1              48               2      32512             cpuII     2018/01/03 12:48:46     2018/01/03 12:48:48
6870080   0             24          1             336              14      32512             cpuII     2018/01/03 19:36:31     2018/01/03 19:36:45
6976238   0             24          1          518976           21624      35840             cpuII     2018/01/08 10:53:15     2018/01/08 16:53:39
7049147   0             24          2           15936            1328          0            c_blsc     2018/01/11 11:01:31     2018/01/11 11:12:35
7049161   0             20          1         1708760           85438          0            c_blsc     2018/01/11 11:13:10     2018/01/12 10:57:08
7087083   0             24          1              24               1      32256             cpuII     2018/01/30 13:57:08     2018/01/30 13:57:09
7087102   0             24          1             768              32          0             cpuII     2018/01/30 14:03:50     2018/01/30 14:04:22
