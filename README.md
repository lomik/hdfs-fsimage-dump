# hdfs-fsimage-dump
Dump files and directories from HDFS fsimage to json

Difference from `hdfs oiv -p Delimited`:
* Removed files (from "snapshot") are placed into virtual directory "/detached/"

## Build
```sh
git clone https://github.com/lomik/hdfs-fsimage-dump
cd hdfs-fsimage-dump
make submodules
make
```

## Run
```
> ./hdfs-fsimage-dump fsimage_0000000004857320956

{"Group":"hadoop","ModificationTime":1504563295546,"Path":"/var/log/hadoop-yarn/apps/jenkins/logs/application_1504003800323_11550","Permission":"-rwxrwx---","User":"jenkins"}
{"AccessTime":1504562040592,"BlocksCount":1,"FileSize":10382,"Group":"hadoop","ModificationTime":1504562040782,"Path":"/var/log/hadoop-yarn/apps/jenkins/logs/application_1504003800323_11541/hp0_45454","Permission":"-rw-r-----","PreferredBlockSize":536870912,"Replication":3,"User":"jenkins"}
```

