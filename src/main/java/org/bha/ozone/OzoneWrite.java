package org.bha.ozone;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.util.Time;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class OzoneWrite {

  public static OmMetadataManagerImpl omMetadataManager;
  public static OzoneConfiguration ozoneConfiguration;
  public static KeyManager keyManager;
  public static MiniOzoneCluster cluster;
  public static String path;
  public static String volumeName = UUID.randomUUID().toString();
  public static String bucketName = UUID.randomUUID().toString();

  public static void setOM(String path) throws Exception {
    OzoneWrite.path = path;
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set("om.db.dirs", path);
    cluster = MiniOzoneCluster.newBuilder(ozoneConfiguration)
        .setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    keyManager = cluster.getOzoneManager().getKeyManager();

  }


  public static void setup() throws Exception {
    ObjectStore store = cluster.getClient().getObjectStore();
    store.createVolume(volumeName);
    store.getVolume(volumeName).createBucket(bucketName);
  }

  public static void shutdown()  {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  public static long doWork(long count) throws Exception {

  /*  int allocatedsize = 256;
    AllocatedBlock allocatedBlock =
        cluster.getStorageContainerManager().getBlockProtocolServer().allocateBlock(
            allocatedsize, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, UUID.randomUUID().toString());

    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();

    OmKeyLocationInfo.Builder builder = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(allocatedBlock.getBlockID()))
        .setLength(allocatedsize)
        .setOffset(0);
    omKeyLocationInfoList.add(builder.build());

    allocatedBlock =
        cluster.getStorageContainerManager().getBlockProtocolServer().allocateBlock(
            allocatedsize, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, UUID.randomUUID().toString());

    builder = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(allocatedBlock.getBlockID()))
        .setLength(allocatedsize)
        .setOffset(0);
    omKeyLocationInfoList.add(builder.build());*/

    long startTime;

    startTime = System.currentTimeMillis();

    for(long i=0; i < count; i++) {
      // setting data size to zero to avoid call for scm for allocate block
      OmKeyArgs omKeyArgs =
          new OmKeyArgs.Builder().setVolumeName(volumeName)
              .setBucketName(bucketName)
              .setKeyName(UUID.randomUUID().toString())
              .setDataSize(0)
              .setFactor(HddsProtos.ReplicationFactor.THREE)
              .setType(HddsProtos.ReplicationType.RATIS).build();

      OpenKeySession openKeySession = keyManager.openKey(omKeyArgs);

      // setting location info list
      omKeyArgs.setLocationInfoList(getKeyInfoList());
      keyManager.commitKey(omKeyArgs, openKeySession.getId());
    }

    long endTime = (System.currentTimeMillis() - startTime)/1000;
    System.out.println("Time Taken by thread "  +
        Thread.currentThread().getName() + "is " +
        endTime +" seconds");

    return endTime;

  }

  public static List<OmKeyLocationInfo> getKeyInfoList() {
    List<OmKeyLocationInfo> omKeyLocationInfoList= new ArrayList<>();

    omKeyLocationInfoList.add(getKeyInfo());
    omKeyLocationInfoList.add(getKeyInfo());

    return omKeyLocationInfoList;

  }

  public static OmKeyLocationInfo getKeyInfo() {
    return  new OmKeyLocationInfo.Builder().setBlockID(
        new BlockID(RandomUtils.nextLong(0, 100000000),
            RandomUtils.nextLong(0, 10000000)))
        .setLength(RandomUtils.nextLong(0, 10000000))
        .setOffset(0).build();
  }

}
