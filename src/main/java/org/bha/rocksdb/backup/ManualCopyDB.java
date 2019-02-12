package org.bha.rocksdb.backup;

import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.Options;
import org.rocksdb.RestoreOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.io.File;

public class ManualCopyDB {

  public static void manulCopy(String args[]) throws Exception {

    RocksDB.loadLibrary();

    Options options = new Options().setCreateIfMissing(true);
    try(BackupableDBOptions backupableDBOptions = new BackupableDBOptions(
        "/tmp/rocksdb-bkp1"); BackupEngine be =
        BackupEngine.open(options.getEnv(), backupableDBOptions)) {


      be.restoreDbFromBackup(be.getBackupInfo().get(4).backupId(), "/tmp/new"
          , "/tmp/new",
          new RestoreOptions(false));

      File f = new File("/tmp/new");
      f.mkdirs();

      RocksDB db = RocksDB.open(options,"/tmp/new");

      int count = 0;
      RocksIterator rocksDBIterator = db.newIterator();
      rocksDBIterator.seekToFirst();
      while(rocksDBIterator.isValid()) {
        rocksDBIterator.next();
        count++;
      }

      System.out.println(count);

      System.out.println(new String(db.get("key3".getBytes())));
      System.out.println(new String(db.get("key4".getBytes())));
      System.out.println(new String(db.get("key1".getBytes())));
      System.out.println(new String(db.get("key2".getBytes())));

    }
  }
}
