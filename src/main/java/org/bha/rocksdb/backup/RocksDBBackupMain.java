package org.bha.rocksdb.backup;

import org.rocksdb.RocksDB;

public class RocksDBBackupMain {


  public static void main(String args[]) throws Exception {

    if (args[1].equals("setup")) {
      String dbFolder = "/tmp/rocksdb";
      String backupFolder = "/tmp/rocksdb-bkp";
      String afterbkpFolder = "/tmp/rocksdb-afterbkp";
      RocksDBBackup.DbBackup(dbFolder, backupFolder, afterbkpFolder, true);


      dbFolder = "/tmp/rocksdb1";
      backupFolder = "/tmp/rocksdb-bkp1";
      afterbkpFolder = "/tmp/rocksdb-afterbkp1";
      RocksDBBackup.DbBackup(dbFolder, backupFolder, afterbkpFolder, false);

      System.out.println("Done");
    } else {
      ManualCopyDB.manulCopy(args);
    }

  }
}
