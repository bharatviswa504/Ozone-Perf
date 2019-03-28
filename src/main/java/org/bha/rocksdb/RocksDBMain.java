package org.bha.rocksdb;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class RocksDBMain {

  public static RocksDBWrite rocksDBWrite;
  public static String path;
  public static String volumeName;
  public static String bucketName;

  public static void openDB(String path, boolean sync) throws RocksDBException {
    RocksDBMain.path = path;
    rocksDBWrite = new RocksDBWrite();
    rocksDBWrite.openDB(path, sync);
    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
  }

  public static void deleteDB() {
    if (path != null) {
      FileUtil.fullyDelete(new File(path));
    }
  }


  public static long doWork(long count) throws Exception {
    long startTime = System.currentTimeMillis();
    for (int i=0; i< count; i++) {
      String keyName = UUID.randomUUID().toString();

      OmKeyInfo omKeyInfo =
          new OmKeyInfo.Builder().setVolumeName(volumeName)
              .setBucketName(bucketName)
              .setKeyName(keyName)
              .setReplicationFactor(HddsProtos.ReplicationFactor.THREE)
              .setReplicationType(HddsProtos.ReplicationType.RATIS)
              .setOmKeyLocationInfos(Lists.newArrayList(
                  new OmKeyLocationInfoGroup(0,
                      rocksDBWrite.getKeyInfoList()))).build();

      byte[] key =
          rocksDBWrite.getKey(volumeName, bucketName, keyName).getBytes();
      byte[] val = omKeyInfo.getProtobuf().toByteArray();

      // Doing similar what we do in OM
      rocksDBWrite.put(key, val);
      rocksDBWrite.commit(key, val);

    }

    System.out.println("Time taken by thread " +
            Thread.currentThread().getName() + " is :" +
        (System.currentTimeMillis() - startTime)/1000);

    return (System.currentTimeMillis() - startTime)/1000;

  }


  public static long doWork1(long count, boolean batch) throws Exception {
    long startTime = System.currentTimeMillis();

    int random = 0;
    if (!batch) {
      System.out.println("RocksDB with out batch");
      for (int i = 0; i < count; i++) {
        String keyName = Integer.toString(i);

        OmKeyInfo omKeyInfo =
            new OmKeyInfo.Builder().setVolumeName(volumeName)
                .setBucketName(bucketName)
                .setKeyName(keyName)
                .setReplicationFactor(HddsProtos.ReplicationFactor.THREE)
                .setReplicationType(HddsProtos.ReplicationType.RATIS)
                .setOmKeyLocationInfos(Lists.newArrayList(
                    new OmKeyLocationInfoGroup(0,
                        rocksDBWrite.getKeyInfoList()))).build();

        byte[] key = //UUID.randomUUID().toString().getBytes();
           rocksDBWrite.getKey(volumeName, bucketName, keyName).getBytes();
        byte[] val = //UUID.randomUUID().toString().getBytes();
        omKeyInfo.getProtobuf().toByteArray();

        // Doing similar what we do in OM
        //rocksDBWrite.put(key, val);

        rocksDBWrite.doDoubleBuffer2(key, val);
      //  rocksDBWrite.commit(key, val);

      }

      long futureCount = 0;
      for (CompletableFuture<Integer> future : rocksDBWrite.getFutureList()) {
        futureCount += future.get();
      }
      System.out.println("Future count is " + futureCount);
      System.out.println("Given count is " + count);
      System.out.println("Future size list is" + rocksDBWrite.getFutureList().size());
    } else {
      System.out.println("RocksDB with batch");
      for (int i = 0; i < count; i++) {
        String keyName = Integer.toString(i);

        OmKeyInfo omKeyInfo =
            new OmKeyInfo.Builder().setVolumeName(volumeName)
                .setBucketName(bucketName)
                .setKeyName(keyName)
                .setReplicationFactor(HddsProtos.ReplicationFactor.THREE)
                .setReplicationType(HddsProtos.ReplicationType.RATIS)
                .setOmKeyLocationInfos(Lists.newArrayList(
                    new OmKeyLocationInfoGroup(0,
                        rocksDBWrite.getKeyInfoList()))).build();

        byte[] key = //UUID.randomUUID().toString().getBytes();
         rocksDBWrite.getKey(volumeName, bucketName, keyName).getBytes();
        byte[] val = //UUID.randomUUID().toString().getBytes();
        omKeyInfo.getProtobuf().toByteArray();

        rocksDBWrite.doDoubleBuffer1(key, val);

      }

      long futureCount = 0;
      for (CompletableFuture<Integer> future : rocksDBWrite.getFutureList()) {
        futureCount += future.get();
      }
      System.out.println("Future count is " + futureCount);
      System.out.println("Given count is " + count);
      System.out.println("Future size list is" + rocksDBWrite.getFutureList().size());

    }

    System.out.println("Time taken by thread " +
        Thread.currentThread().getName() + " is :" +
        (System.currentTimeMillis() - startTime)/1000);

    return (System.currentTimeMillis() - startTime)/1000;

  }

  public static long getCount() {
    return rocksDBWrite.getCount();
  }

  public static void flush() throws Exception{
    rocksDBWrite.flush();
  }

}
