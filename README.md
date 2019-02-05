This is repository contains code for measuring ozone (OM) and rocksdb performance.

Compile:
mvn clean install

Run:
java -jar target/rocksdb-1.0-SNAPSHOT-jar-with-dependencies.jar <<num of
threads>> <<number of keys per each thread>> <<db path:default -
/tmp/testrocksdb.db>>
