package org.bha.rocksdb.snapshot;

import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;

public class SnapShot {

  public static void main(String args[]) throws Exception {

    String dbFolder = "/tmp/rocksdb-c";
    String checkpointFolder = "/tmp/rocksdb-checkpoint";

    File f = new File(dbFolder);
    f.mkdirs();

    f = new File(checkpointFolder);
    f.mkdirs();
   try(Options options = new Options().
       setCreateIfMissing(true)) {
     try (final RocksDB db = RocksDB.open(options, dbFolder)) {
       db.put("key".getBytes(), "value".getBytes());

       try (final Checkpoint checkpoint = Checkpoint.create(db)) {
         checkpoint.createCheckpoint(checkpointFolder + "/snapshot1");
         db.put("key2".getBytes(), "value2".getBytes());
         checkpoint.createCheckpoint(checkpointFolder+ "/snapshot2");
         System.out.print("bharat");
       }
     }
   }
  }
}
