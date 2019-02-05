package org.bha.rocksdb;

import com.google.common.collect.Lists;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.UUID;

public class RocksDBMain {

  public static RocksDBWrite rocksDBWrite;
  public static String path;
  public static String volumeName;
  public static String bucketName;

  public static void openDB(String path) throws RocksDBException {
    RocksDBMain.path = path;
    rocksDBWrite = new RocksDBWrite();
    rocksDBWrite.openDB(path);
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

}
