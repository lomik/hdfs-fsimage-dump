# hdfs-fsimage-dump
Dump files, directories and snapshotted directories from HDFS fsimage to json

Difference from `hdfs oiv -p Delimited`:
* Snapshotted directories dump added
* [optional] -extra-fields: extra custom static json fields can be added to result json
* [optional] -snap-replace: snapshots are placed into virtual directory /(snapshots)
* [optional] -snap-cleanup: snapshots will contain only deleted object(s)
* Lost files are placed into virtual directory "/(detached)" or "../(unknown)/.." BUG?

## Build
```sh
git clone https://github.com/lomik/hdfs-fsimage-dump
cd hdfs-fsimage-dump
make submodules
make
```

## Run
```
> ./hdfs-fsimage-dump -i fsimage_0000000004857320956 -extra-fields {\"data\":\"2017-09-09\"}

{"Group":"hadoop","ModificationTime":"2017-09-18 12:05:39","ModificationTimeMs":1505725539089,"Path":"/var/log/hadoop-yarn/apps/jenkins/logs/application_1504003800323_11550","Permission":"-rwxrwx---","User":"jenkins","data":"2017-09-09"}
{"AccessTime":"2017-09-14 19:06:29","AccessTimeMs":1505405189045,"BlocksCount":1,"FileSize":10382,"Group":"hadoop","ModificationTime":"2017-09-14 19:06:30","ModificationTimeMs":1505405190268,"Path":"/var/log/hadoop-yarn/apps/jenkins/logs/application_1504003800323_11541/hp0_45454","Permission":"-rw-r-----","PreferredBlockSize":536870912,"Replication":3,"User":"jenkins","data":"2017-09-09"}
{"AccessTime":"2017-09-18 12:05:07","AccessTimeMs":1505725507232,"BlocksCount":1,"FileSize":114819072,"Group":"hadoop","ModificationTime":"2017-09-18 12:05:08","ModificationTimeMs":1505725508395,"Path":"/tmp/.snapshot/testsnap_201070918/del_snap/snap_20170918.bin","Permission":"-rw-r--r--","PreferredBlockSize":536870912,"Replication":3,"User":"hdfs","data":"2017-09-09"}
```

