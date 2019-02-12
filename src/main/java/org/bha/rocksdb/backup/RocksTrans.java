package org.bha.rocksdb.backup;

import org.apache.hadoop.fs.FileUtil;
import org.apache.zookeeper.Op;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.awt.*;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RocksTrans {

  public static void main(String args[]) throws Exception {

    String dbFolder1 = "/tmp/dbFolder1";
    String dbFolder2 = "/tmp/dbFolder2";
    RocksDB db1 = null;
    RocksDB db2 = null;
    try {
      Options opt = new Options().setCreateIfMissing(true);

      opt.setWalSizeLimitMB(128);
      opt.setWalTtlSeconds(60 * 60);

      File f = new File(dbFolder1);
      f.mkdirs();

      f = new File(dbFolder2);
      f.mkdirs();

      db1 = RocksDB.open(opt,
          dbFolder1);

      db2 = RocksDB.open(opt, dbFolder2);


      long startTransaction = db1.getLatestSequenceNumber();

      System.out.println("DB1 Sequence no " + db1.getLatestSequenceNumber());
      System.out.println("DB2 Sequence no " + db1.getLatestSequenceNumber());

      // Write some transactions to Db


      db1.put("key1".getBytes(), "key2".getBytes());
      db1.put("key2".getBytes(), "key3".getBytes());

      System.out.println("After 2 transactions DB1 Latest Seq no " +
          db1.getLatestSequenceNumber());

      TransactionLogIterator transactionLogIterator;
      if (startTransaction == 0) {
        transactionLogIterator =
            db1.getUpdatesSince(startTransaction);
      } else {
        transactionLogIterator =
            db1.getUpdatesSince(startTransaction + 1);
      }


      // Applying transactions to db2
     long count = 0;
      while(transactionLogIterator.isValid()) {
        //*System.out.println(count++);*//
        TransactionLogIterator.BatchResult result =
            transactionLogIterator.getBatch();
        System.out.println(result.sequenceNumber());
        WriteBatch writeBatch = result.writeBatch();
        db2.write(new WriteOptions(),
            writeBatch);
        transactionLogIterator.next();

      }


      startTransaction = db1.getLatestSequenceNumber() + 1;


      db1.put("key1".getBytes(), "key3".getBytes());
      db1.put("key2".getBytes(), "key4".getBytes());

      transactionLogIterator =
          db1.getUpdatesSince(startTransaction);


      List<ByteBuffer> byteBuffers = new ArrayList<>();

      while(transactionLogIterator.isValid()) {
        TransactionLogIterator.BatchResult result =
            transactionLogIterator.getBatch();

        System.out.println(result.sequenceNumber());
        WriteBatch writeBatch = result.writeBatch();
        ByteBuffer byteBuffer =ByteBuffer.allocate(writeBatch.data().length);
        byteBuffer.put(writeBatch.data());
        byteBuffers.add(byteBuffer);
        transactionLogIterator.next();
      }


      for ( ByteBuffer byteBuffer : byteBuffers) {
       db2.write(new WriteOptions(), new WriteBatch(byteBuffer.array()));
      }


      System.out.println("Key1 val from db2 " + new String(db2.get("key1".getBytes())));
      System.out.println("Key2 val from db2 " + new String(db2.get("key2".getBytes())));

      System.out.println("DB1 sequence no " + db1.getLatestSequenceNumber());
      System.out.println("DB2 sequence no " + db2.getLatestSequenceNumber());


      startTransaction = db2.getLatestSequenceNumber();

      db1.delete("key1".getBytes());

      System.out.println("DB1 sequence no after delete is" + db1.getLatestSequenceNumber());



      transactionLogIterator = db1.getUpdatesSince(startTransaction + 1);
      while(transactionLogIterator.isValid()) {
        /*System.out.println(count++);*/
        WriteBatch writeBatch = transactionLogIterator.getBatch().writeBatch();
        db2.write(new WriteOptions(),
            writeBatch);
        transactionLogIterator.next();

      }

      System.out.println("DB1 sequence no " + db1.getLatestSequenceNumber());
      System.out.println("DB2 sequence no " + db2.getLatestSequenceNumber());


    } finally {
      if (db1 != null) {
        db1.close();
      }
       if (db2 != null) {
         db2.close();
       }
      FileUtil.fullyDelete(new File(dbFolder1));
      FileUtil.fullyDelete(new File(dbFolder2));
    }


  }

  private void doWrite(RocksDB db) throws RocksDBException  {

    for (int i=0; i< 1000; i++) {
      db.put("key1".getBytes(), "key2".getBytes());
      db.put("key2".getBytes(), "key3".getBytes());
    }
  }
}
