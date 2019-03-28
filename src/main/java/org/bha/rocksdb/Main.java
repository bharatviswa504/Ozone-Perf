package org.bha.rocksdb;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.bha.ozone.OzoneWrite;
import picocli.CommandLine;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main implements Callable<Void> {

  @CommandLine.Parameters(index = "0", description = "Number of Threads")
  private int numThreads;

  @CommandLine.Parameters(index = "1", description = "Number of keys per each" +
      " thread")
  private long numKeys;

  @CommandLine.Parameters(index = "2", description = "Test Ozone perf/ Test " +
      "rocksdb perf", defaultValue = "rocksdb")
  private String type;

  @CommandLine.Parameters(index = "3", description = "Sync flag",
      defaultValue = "false")
  private boolean sync;

  @CommandLine.Parameters(index = "4", description = "Batch flag",
      defaultValue = "false")
  private boolean batch;

  @CommandLine.Parameters(index = "5",  description = "DB Location",
      defaultValue = "/tmp/testrocksdb.db")
  private String path;


  public static void main(String[] args) throws Exception {
    CommandLine.call(new Main(), args);
  }

  public Void call() throws Exception {
    // Checking values
    if (numThreads <= 0) {
      numThreads = 1;
    }

    if (numKeys <=0) {
      numKeys = 100000;
    }

    if (type.equals("ozone")) {
      OzoneWrite.setOM(path);
      OzoneWrite.setup();

      // Start the threads.
      final ExecutorService executor = Executors.newFixedThreadPool(
          numThreads,
          (new ThreadFactoryBuilder().setNameFormat(
              "Ozone-Thread-%d").build()));

      final CompletionService<Object> ecs =
          new ExecutorCompletionService<>(executor);


      long startTime = System.currentTimeMillis();
      for (long t = 0; t < numThreads; t++) {
        ecs.submit(() -> {
          long time = 0;
          try {
            time = OzoneWrite.doWork(numKeys);
          } catch (Exception ex) {
            System.out.println(ex);
          }
          return time;
        });
      }

      long totalTime = 0L;
      // And wait for all threads to complete.
      for (long t = 0; t < numThreads; t++) {
        totalTime += (long) ecs.take().get();
      }
      executor.shutdown();
      OzoneWrite.shutdown();


      System.out.println("Time taken in Main is " +
          (System.currentTimeMillis() - startTime) / 1000);
      System.out.println("Total time taken by all threads is" + totalTime);
      System.out.print("DB count is " + RocksDBMain.getCount());

    } else {
      // Open DB
      RocksDBMain.openDB(path, sync);

      // Start the threads.
      final ExecutorService executor = Executors.newFixedThreadPool(
          numThreads,
          (new ThreadFactoryBuilder().setNameFormat(
              "RocksDB-Thread-%d").build()));

      final CompletionService<Object> ecs =
          new ExecutorCompletionService<>(executor);


      long startTime = System.currentTimeMillis();
      for (long t = 0; t < numThreads; t++) {
        ecs.submit(() -> {
          long time = 0;
          try {
            time = RocksDBMain.doWork1(numKeys, batch);
          } catch (Exception ex) {
            System.out.println(ex);
          }
          return time;
        });
      }



      long totalTime = 0L;
      // And wait for all threads to complete.
      for (long t = 0; t < numThreads; t++) {
        totalTime += (long) ecs.take().get();
      }
      executor.shutdown();

      System.out.println("Time taken in Main is " +
          (System.currentTimeMillis() - startTime) / 1000);

      System.out.println("Total time taken by all threads is" + totalTime);

      //RocksDBMain.flush();
      System.out.print("DB count is " + RocksDBMain.getCount());

      // Finally delete the db
     RocksDBMain.deleteDB();
    }

    return null;

  }

}
