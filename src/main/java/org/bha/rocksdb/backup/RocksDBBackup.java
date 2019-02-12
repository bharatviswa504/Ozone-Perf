package org.bha.rocksdb.backup;

import org.apache.hadoop.fs.FileUtil;
import org.bha.rocksdb.RocksDBMain;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupInfo;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.Options;
import org.rocksdb.RestoreOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class RocksDBBackup {

  public static void DbBackup(String dbFolder, String backupFolder,
      String afterbkpFolder, boolean moreWrites) throws Exception {

    File f = new File(dbFolder);
    f.mkdirs();

    f = new File(backupFolder);
    f.mkdirs();

    f = new File(afterbkpFolder);
    f.mkdirs();

    try(final Options opt =
            new Options().setCreateIfMissing(true)) {
      RocksDB db = null;
      try {
        // Open empty database.
        db = RocksDB.open(opt,
            dbFolder);
        // Fill database with some test values
        prepareDatabase(db);
        try (final BackupableDBOptions bopt = new BackupableDBOptions(
            backupFolder); final BackupEngine be = BackupEngine.open(opt.getEnv(), bopt);) {

          // create first backup
          be.createNewBackup(db, false);

          System.out.println("Backup list size, after first backup" +
              be.getBackupInfo().size());

          db.put("key3".getBytes(), "b3".getBytes());
          db.put("key4".getBytes(), "b4".getBytes());

          // create second backup
          be.createNewBackup(db, false);

          System.out.println("Backup list size, after second backup" +
              be.getBackupInfo().size());

          List<BackupInfo> backupInfo = be.getBackupInfo();
          // delete first backup
          be.deleteBackup(backupInfo.get(0).backupId());

          System.out.println("Backup size, after deleting first backup" +
              be.getBackupInfo().size());


         /* System.out.println("Restore from 2nd backup");
          // restore db from first backup
          be.restoreDbFromBackup(backupInfo.get(1).backupId(),
              afterbkpFolder,
              afterbkpFolder,
              new RestoreOptions(false));

          // Open database again.
         RocksDB db1 = RocksDB.open(opt, afterbkpFolder);

          System.out.println(new String(db1.get("key3".getBytes())));
          System.out.println(new String(db1.get("key4".getBytes())));
          System.out.println(new String(db1.get("key1".getBytes())));
          System.out.println(new String(db1.get("key2".getBytes())));
          db1.close();*/

          //

          db.put("key3".getBytes(), "b3-m".getBytes());

          be.createNewBackup(db, false);

          System.out.println("Backup list size, after third backup" +
              be.getBackupInfo().size());

         /* backupInfo = be.getBackupInfo();
          //load from latest backup
          be.restoreDbFromBackup(backupInfo.get(1).backupId(),
              afterbkpFolder,
              afterbkpFolder,
              new RestoreOptions(false));
          // Open database again.
          db1 = RocksDB.open(opt, afterbkpFolder);

          System.out.println("Load from after latest bkp 3rd");

          System.out.println(new String(db1.get("key3".getBytes())));
          System.out.println(new String(db1.get("key4".getBytes())));
          System.out.println(new String(db1.get("key1".getBytes())));
          System.out.println(new String(db1.get("key2".getBytes())));

          db1.close();*/

         if(moreWrites) {
           doSomeWrites(db);
           be.createNewBackup(db, false);


           doSomeWrites(db);
           be.createNewBackup(db, false);


           doSomeWrites(db);
           be.createNewBackup(db, false);

           RocksIterator rocksDBIterator = db.newIterator();
           rocksDBIterator.seekToFirst();

           int count = 0;
           while(rocksDBIterator.isValid()) {
             rocksDBIterator.next();
             count++;
           }

           System.out.println("Key count in DB" + count);

         }
        }



/*        System.out.println("Creating backup Engine from back up folder and " +
            "trying to open db");

        try (final BackupableDBOptions bopt1 = new BackupableDBOptions(
            backupFolder); final BackupEngine be1 =
            BackupEngine.open(opt.getEnv(), bopt1);) {

          System.out.println("Backup list size" + be1.getBackupInfo().size());

          be1.restoreDbFromBackup(be1.getBackupInfo().get(1).backupId(), afterbkpFolder,
              afterbkpFolder, new RestoreOptions(false));

          RocksDB rocksDB = RocksDB.open(opt,  afterbkpFolder);


          System.out.println(new String(rocksDB.get("key3".getBytes())));
          System.out.println(new String(rocksDB.get("key4".getBytes())));
          System.out.println(new String(rocksDB.get("key1".getBytes())));
          System.out.println(new String(rocksDB.get("key2".getBytes())));

          rocksDB.close();

        }*/

      } finally {
        if(db != null) {
          db.close();
/*         FileUtil.fullyDelete(new File(dbFolder));
         FileUtil.fullyDelete(new File(backupFolder));
         FileUtil.fullyDelete(new File(afterbkpFolder));*/
        }
      }



    }
  }

  private static void prepareDatabase(final RocksDB db)
      throws RocksDBException {
    db.put("key1".getBytes(), "b1".getBytes());
    db.put("key2".getBytes(), "b2".getBytes());
  }


  private static void doSomeWrites(final RocksDB db)
      throws RocksDBException {

    for (int i=0; i< 1000; i++) {
      db.put(UUID.randomUUID().toString().getBytes(), UUID.randomUUID().toString().getBytes());
      db.put(UUID.randomUUID().toString().getBytes(), UUID.randomUUID().toString().getBytes());
    }
  }
}
