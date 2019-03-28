package org.bha.rocksdb;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class RocksDBWrite {

  private RocksDB db;

  private ColumnFamilyHandle defaultHanlde;
  private ColumnFamilyHandle moveHandle;
  public WriteOptions writeOptions;

  private Map<byte[], byte[]> map1 = new ConcurrentHashMap<>();
  private Map<byte[], byte[]> map2 = new ConcurrentHashMap<>();

  private volatile AtomicBoolean canCommit = new AtomicBoolean(true);
  private volatile AtomicBoolean canAppendTomap1 = new AtomicBoolean(true);
 // private volatile AtomicBoolean canAppendTomap2 = new AtomicBoolean(true);

  private List<CompletableFuture<Integer>> futureList = new ArrayList<>();

  public List<CompletableFuture<Integer>> getFutureList() {
    return futureList;
  }

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

    System.out.println("sync set to " + sync);
    writeOptions = new WriteOptions().setSync(sync);

  }

  public void put(byte[] key, byte[] value) throws RocksDBException  {
    db.put(defaultHanlde, key, value);
  }


  public void commit(byte[] key, byte[] value) throws RocksDBException {
      WriteBatch writeBatch = new WriteBatch();
      writeBatch.put(moveHandle, key, value);
      writeBatch.delete(defaultHanlde, key);
      db.write(writeOptions, writeBatch);
  }

  public void commit(WriteBatch writeBatch, byte[] key, byte[] value) throws RocksDBException {
    writeBatch.put(defaultHanlde, key, value);
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
        .setOffset(0).setPipeline( Pipeline.newBuilder().setId(PipelineID.randomId())
          .setType(HddsProtos.ReplicationType.RATIS)
          .setFactor(HddsProtos.ReplicationFactor.THREE)
          .setState(Pipeline.PipelineState.OPEN)
          .setNodes(new ArrayList<>()).build()).build();
  }


  public long getCount() {
    RocksIterator rocksIterator = db.newIterator(defaultHanlde);
    rocksIterator.seekToFirst();
    long count = 0;
    while(rocksIterator.isValid()) {
      count ++;
      rocksIterator.next();
    }
    return count;
  }


/*

  public void doDoubleBuffer(byte[] key, byte[] val) {

    if(canAppendTomap1.get()) {
      map1.put(key, val);
      if (canCommit.get()) {
        canCommit.set(true);
        canAppendTomap1.set(false);
      CompletableFuture<Integer> future =  CompletableFuture.supplyAsync(() -> {
          WriteBatch writeBatch = new WriteBatch();
          int count = map1.size();
          map1.forEach( (key1, val1) -> {
            try {
              commit(writeBatch, key1, val1);
            } catch (RocksDBException ex) {
              System.out.println(ex);
            }
          });
          try {
            db.write(writeOptions, writeBatch);
          } catch (RocksDBException ex) {
            System.out.println(ex);
          }
          map1.clear();
          canCommit.set(true);
          canAppendTomap1.set(true);
          return count;
        });
      futureList.add(future);
      }
    } else if (canAppendTomap2.get()) {
      map2.put(key, val);
      if (canCommit.get()) {
        canAppendTomap2.set(false);
        canCommit.set(false);
        CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
          WriteBatch writeBatch = new WriteBatch();
          int count = map2.size();
          map2.forEach((key1, val1) -> {
            try {
              commit(writeBatch, key1, val1);
            } catch (RocksDBException ex) {
              System.out.println(ex);
            }
          });
          try {
            db.write(writeOptions, writeBatch);
          } catch (RocksDBException ex) {
            System.out.println(ex);
          }
          map2.clear();
          canCommit.set(true);
          canAppendTomap2.set(true);
          return count;
        });
        futureList.add(future);
      }
    }



  }
*/


  public void doDoubleBuffer1(byte[] key, byte[] val) {

    if (canAppendTomap1.get()) {
      if (map1.get(key) == null) {
        map1.put(key, val);
      } else {
        System.out.print("Got duplicate");
      }
    } else {
      if(map2.get(key) == null) {
        map2.put(key, val);
      } else {
        System.out.print("Got duplicate");
      }
    }

    if (canCommit.get()) {
      if (canAppendTomap1.get()) {
        canCommit.set(false);
        canAppendTomap1.set(false);
        if (map2.size() > 0) {
          throw new IllegalArgumentException("Map2 size should be zero");
        }
       /* CompletableFuture<Integer> future =
            CompletableFuture.supplyAsync(() -> batchCommit1(map1));*/
        CompletableFuture<Integer> future =
            CompletableFuture.supplyAsync(() -> {
              WriteBatch writeBatch = new WriteBatch();
              int count = map1.size();
              map1.forEach((key1, val1) -> {
                try {
                  commit(writeBatch, key1, val1);
                } catch (RocksDBException ex) {
                  System.out.println(ex);
                }
              });
              try {
                db.write(writeOptions, writeBatch);
              } catch (RocksDBException ex) {
                System.out.println(ex);
              }
              map1.clear();
              canCommit.set(true);
              return count;
            });
        futureList.add(future);
      } else {
        canCommit.set(false);
        canAppendTomap1.set(true);
        if (map1.size() > 0) {
          throw new IllegalArgumentException("Map1 size should be zero");
        }
       /* CompletableFuture<Integer> future =
            CompletableFuture.supplyAsync(() -> batchCommit1(map2));*/
        CompletableFuture<Integer> future =
            CompletableFuture.supplyAsync(() -> {
              WriteBatch writeBatch = new WriteBatch();
              int count = map2.size();
              map2.forEach((key1, val1) -> {
                try {
                  commit(writeBatch, key1, val1);
                } catch (RocksDBException ex) {
                  System.out.println(ex);
                }
              });
              try {
                db.write(writeOptions, writeBatch);
              } catch (RocksDBException ex) {
                System.out.println(ex);
              }
              map2.clear();
              canCommit.set(true);
              return count;
            });
        futureList.add(future);
      }
    }


  }

  private int batchCommit1(Map<byte[], byte[]> map) {
    WriteBatch writeBatch = new WriteBatch();
    int count = map.size();
    map.forEach((key1, val1) -> {
      try {
        commit(writeBatch, key1, val1);
      } catch (RocksDBException ex) {
        System.out.println(ex);
      }
    });
    try {
      db.write(writeOptions, writeBatch);
    } catch (RocksDBException ex) {
      System.out.println(ex);
    }
    map.clear();
    canCommit.set(true);
    return count;
  }

  public void doDoubleBuffer2(byte[] key, byte[] val) throws IllegalArgumentException {

    if (canAppendTomap1.get()) {
      if (map1.get(key) == null) {
        map1.put(key, val);
      } else {
        System.out.print("Got duplicate");
      }
    } else {
      if(map2.get(key) == null) {
        map2.put(key, val);
      } else {
        System.out.print("Got duplicate");
      }
    }

    if (canCommit.get()) {
      if (canAppendTomap1.get()) {
        canCommit.set(false);
        canAppendTomap1.set(false);
        if (map2.size() > 0) {
          throw new IllegalArgumentException("Map2 size should be zero");
        }
/*        CompletableFuture<Integer> future =
            CompletableFuture.supplyAsync(() -> batchCommit2(map1));*/
        CompletableFuture<Integer> future =
            CompletableFuture.supplyAsync(() -> {
              int count = map1.size();
              map1.forEach((key1, val1) -> {
                try {
                  db.put(key1, val1);
                } catch (RocksDBException ex) {
                  System.out.println(ex);
                }
              });
              map1.clear();
              canCommit.set(true);
              return count;
            });
        futureList.add(future);
      } else {
        canCommit.set(false);
        canAppendTomap1.set(true);
        if (map1.size() > 0) {
          throw new IllegalArgumentException("Map1 size should be zero");
        }
        /*        CompletableFuture<Integer> future =
            CompletableFuture.supplyAsync(() -> batchCommit2(map2));*/
        CompletableFuture<Integer> future =
            CompletableFuture.supplyAsync(() -> {
              int count = map2.size();
              map2.forEach((key1, val1) -> {
                try {
                  db.put(key1, val1);
                } catch (RocksDBException ex) {
                  System.out.println(ex);
                }
              });
              map2.clear();
              canCommit.set(true);
              return count;
            });
        futureList.add(future);
      }
    }


  }

  private int batchCommit2(Map<byte[], byte[]> map) {
    int count = map.size();
    map.forEach((key1, val1) -> {
      try {
        db.put(key1, val1);
      } catch (RocksDBException ex) {
        System.out.println(ex);
      }
    });
    map.clear();
    canCommit.set(true);
    return count;
  }

  public void flush() throws Exception{
    db.flush(new FlushOptions());
  }



}
