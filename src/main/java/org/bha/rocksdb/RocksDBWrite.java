package org.bha.rocksdb;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;

public class RocksDBWrite {

  private RocksDB db;

  private ColumnFamilyHandle defaultHanlde;
  private ColumnFamilyHandle moveHandle;
  public WriteOptions writeOptions;

  public void openDB(String path, boolean sync) throws RocksDBException {

    DBOptions dbOptions = getDBOptions();

    ColumnFamilyDescriptor columnFamilyDescriptor1 =
        new ColumnFamilyDescriptor("default".getBytes(),
            getColumnFamilyOptions());

    ColumnFamilyDescriptor columnFamilyDescriptor2 =
        new ColumnFamilyDescriptor("move".getBytes(),
            getColumnFamilyOptions());

    List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        new ArrayList<ColumnFamilyDescriptor>();

    List<ColumnFamilyHandle> columnFamilyHandles =
        new ArrayList<ColumnFamilyHandle>();

    columnFamilyDescriptors.add(columnFamilyDescriptor1);
    columnFamilyDescriptors.add(columnFamilyDescriptor2);

    db = RocksDB.open(dbOptions, path, columnFamilyDescriptors, columnFamilyHandles);

    defaultHanlde = columnFamilyHandles.get(0);
    moveHandle = columnFamilyHandles.get(1);

    System.out.print("sync set to " + sync);
    writeOptions = new WriteOptions().setSync(sync);

  }

  public void put(byte[] key, byte[] value) throws RocksDBException  {
    db.put(defaultHanlde, writeOptions, key, value);
  }


  public void commit(byte[] key, byte[] value) throws RocksDBException {
      WriteBatch writeBatch = new WriteBatch();
      writeBatch.put(moveHandle, key, value);
      writeBatch.delete(defaultHanlde, key);
      db.write(writeOptions, writeBatch);
  }


  public ColumnFamilyOptions getColumnFamilyOptions() {
    // Set BlockCacheSize to 256 MB. This should not be an issue for HADOOP.
    final long blockCacheSize = 256 * 1024 * 1024;

    // Set the Default block size to 16KB
    final long blockSize = 16 * 1024;

    // Write Buffer Size -- set to 128 MB
    final long writeBufferSize = 128 * 1024 * 1024;

    return new ColumnFamilyOptions()
        .setLevelCompactionDynamicLevelBytes(true)
        .setWriteBufferSize(writeBufferSize)
        .setTableFormatConfig(
            new BlockBasedTableConfig()
                .setBlockCacheSize(blockCacheSize)
                .setBlockSize(blockSize)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true)
                .setFilter(new BloomFilter())).setCompactionStyle(CompactionStyle.LEVEL);
  }


  public DBOptions getDBOptions() {
    final int maxBackgroundCompactions = 4;
    final int maxBackgroundFlushes = 2;
    final long bytesPerSync = 1 * 1024 * 1024;
    final boolean createIfMissing = true;
    final boolean createMissingColumnFamilies = true;
    return new DBOptions()
        .setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
        .setMaxBackgroundCompactions(maxBackgroundCompactions)
        .setMaxBackgroundFlushes(maxBackgroundFlushes)
        .setBytesPerSync(bytesPerSync)
        .setCreateIfMissing(createIfMissing)
        .setCreateMissingColumnFamilies(createMissingColumnFamilies)
        .setCompactionReadaheadSize( 4 * 1024 * 1024);

  }

  public String getKey(String volumeName, String bucketName, String keyName) {
    return volumeName + "/" + bucketName + "/" + keyName;
  }


  public List<OmKeyLocationInfo> getKeyInfoList() {
    List<OmKeyLocationInfo> omKeyLocationInfoList= new ArrayList<>();

    omKeyLocationInfoList.add(getKeyInfo());
    omKeyLocationInfoList.add(getKeyInfo());

    return omKeyLocationInfoList;

  }

  public OmKeyLocationInfo getKeyInfo() {
  return  new OmKeyLocationInfo.Builder().setBlockID(
        new BlockID(RandomUtils.nextLong(0, 100000000),
            RandomUtils.nextLong(0, 10000000)))
        .setLength(RandomUtils.nextLong(0, 10000000))
        .setOffset(0).build();
  }


}
