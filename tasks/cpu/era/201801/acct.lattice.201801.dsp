
SCCAS accounting information about jobs that are:
  - submitted by users lattice
  - accounted on all projects
  - completed between        Mon Jan  1 00:00:00 2018
                  end        Wed Jan 31 23:59:59 2018
-----------------------------------------------------------------------------------------

SUMMARY:      ( time unit: second )
Total number of completed jobs: 43           Total walltime consumed    : 194708  
Total processors consumed     : 894          Total number of hosts used : 110     
Total wait time in queue      : 3565         Average wait time in queue : 82.9        
-----------------------------------------------------------------------------------------

SCALE SUMMARY:       ( walltime unit: hour )
QueueName              <2       <4       <8      <16      <32      <64     <128     <256     <512    <1024    <2048    <4096    <8192   <16384

cpuII                 0.0      0.0      0.0      0.0      0.0     14.8      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0
cpu_dbg               0.0      0.0      0.0      0.0      0.0      3.4      1.8      0.0      0.0      0.0      0.0      0.0      0.0      0.0
gpu                  34.1      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0      0.0
-----------------------------------------------------------------------------------------

QUEUES SUMMARY:      ( time unit: hour and second )
QueueName           Jobs    CPUs   Hosts  WallTime(h)  HostTime(h)  AverageScale  AvgWaitTime(sec)

cpuII                  9     328      30         14.8          1.5          36.4             148.7
cpu_dbg                8     540      54          5.2          0.5          67.5             246.5
gpu                   26      26      26         34.1         34.1           1.0               9.8
-----------------------------------------------------------------------------------------

Completed jobs list: ( time unit: second )
JobID     GIndex   numProc   numHosts   WallTime(sec)   HostTime(sec)   ExitCode         QueueName               StartTime                 EndTime

6854697   0             24          1              24               1        512             cpuII     2018/01/02 23:46:41     2018/01/02 23:46:42
6854715   0              1          1           18520           18520          0               gpu     2018/01/02 23:47:41     2018/01/03 04:56:21
6869877   0              1          1           21634           21634      35840               gpu     2018/01/03 14:02:59     2018/01/03 20:03:33
6872918   0             80          8             560              56          0           cpu_dbg     2018/01/04 02:18:52     2018/01/04 02:18:59
6872921   0             80          8            2320             232          0           cpu_dbg     2018/01/04 02:29:04     2018/01/04 02:29:33
6872944   0             80          8            1360             136          0           cpu_dbg     2018/01/04 02:39:15     2018/01/04 02:39:32
6872959   0             80          8            2320             232          0           cpu_dbg     2018/01/04 02:49:29     2018/01/04 02:49:58
6872983   0             60          6            1380             138          0           cpu_dbg     2018/01/04 02:59:45     2018/01/04 03:00:08
6872994   0             60          6            3300             330          0           cpu_dbg     2018/01/04 03:10:02     2018/01/04 03:10:57
6873010   0             60          6            3540             354          0           cpu_dbg     2018/01/04 03:20:14     2018/01/04 03:21:13
6873044   0             40          4            4120             412          0           cpu_dbg     2018/01/04 03:40:52     2018/01/04 03:42:35
6874337   0             40          4           12520            1252          0             cpuII     2018/01/04 12:58:59     2018/01/04 13:04:12
6932504   0             40          4             320              32          0             cpuII     2018/01/07 00:44:33     2018/01/07 00:44:41
6938710   0             40          4            9760             976          0             cpuII     2018/01/07 02:20:29     2018/01/07 02:24:33
6963228   0              1          1              30              30      35584               gpu     2018/01/07 17:28:54     2018/01/07 17:29:24
6964167   0              1          1              27              27      35584               gpu     2018/01/08 00:51:45     2018/01/08 00:52:12
6964192   0              1          1              37              37      35584               gpu     2018/01/08 01:11:05     2018/01/08 01:11:42
6964223   0              1          1              34              34      35584               gpu     2018/01/08 01:27:10     2018/01/08 01:27:44
6964246   0              1          1              27              27      35584               gpu     2018/01/08 01:38:58     2018/01/08 01:39:25
6964262   0              1          1              13              13      35584               gpu     2018/01/08 01:46:10     2018/01/08 01:46:23
6964281   0              1          1              15              15      35584               gpu     2018/01/08 01:56:25     2018/01/08 01:56:40
6964327   0              1          1              32              32      35584               gpu     2018/01/08 02:22:41     2018/01/08 02:23:13
6964334   0              1          1              29              29      35584               gpu     2018/01/08 02:25:05     2018/01/08 02:25:34
6964339   0              1          1           17985           17985          0               gpu     2018/01/08 02:28:27     2018/01/08 07:28:12
6990440   0              1          1              35              35      35584               gpu     2018/01/08 21:13:47     2018/01/08 21:14:22
6992346   0              1          1            1886            1886          0               gpu     2018/01/08 22:23:31     2018/01/08 22:54:57
6996000   0              1          1            2023            2023          0               gpu     2018/01/09 00:38:51     2018/01/09 01:12:34
6997362   0             24          1              24               1        512             cpuII     2018/01/09 02:00:58     2018/01/09 02:00:59
6997468   0              1          1              33              33      35584               gpu     2018/01/09 02:08:13     2018/01/09 02:08:46
6997562   0              1          1              28              28      35584               gpu     2018/01/09 02:18:13     2018/01/09 02:18:41
7008441   0              1          1               9               9      35584               gpu     2018/01/09 16:06:07     2018/01/09 16:06:16
7008567   0              1          1              43              43          0               gpu     2018/01/09 16:08:18     2018/01/09 16:09:01
7025458   0              1          1            1239            1239        256               gpu     2018/01/09 23:23:52     2018/01/09 23:44:31
7025545   0              1          1            1198            1198        256               gpu     2018/01/10 00:17:06     2018/01/10 00:37:04
7025598   0              1          1            1271            1271        256               gpu     2018/01/10 00:48:16     2018/01/10 01:09:27
7025651   0              1          1           16973           16973        256               gpu     2018/01/10 01:25:43     2018/01/10 06:08:36
7048577   0              1          1            3151            3151      33280               gpu     2018/01/10 20:59:22     2018/01/10 21:51:53
7048576   0              1          1           17998           17998          0               gpu     2018/01/10 20:58:48     2018/01/11 01:58:46
7048587   0              1          1           18330           18330          0               gpu     2018/01/10 21:55:29     2018/01/11 03:00:59
7068195   0             40          4            1400             140          0             cpuII     2018/01/18 11:47:59     2018/01/18 11:48:34
7068197   0             40          4            9720             972          0             cpuII     2018/01/18 11:52:31     2018/01/18 11:56:34
7068204   0             40          4            9200             920          0             cpuII     2018/01/18 12:12:04     2018/01/18 12:15:54
7068324   0             40          4           10240            1024          0             cpuII     2018/01/18 15:23:53     2018/01/18 15:28:09
