package site.ycsb.db;

import com.c8db.*;
import com.c8db.entity.BaseDocument;
import com.c8db.entity.CollectionEntity;
import com.c8db.entity.MultiDocumentEntity;
import com.c8db.util.MapBuilder;
import site.ycsb.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AsyncC8dbClient {

  /** The database to use. */
  private static String databaseName;

  /** The connection to c8db. */
  private C8DB c8db;

  /** The database to c8db. */
  private C8Database database;

  private Map<String, ByteIterator> result;

  /** The write concern for the requests. */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** The batch size to use for inserts. */
  private static int batchSize;

  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

//  /** The bulk inserts pending for the thread. */
//  private final BatchedWrite.Builder batchedWrite = BatchedWrite.builder()
//      .mode(BatchedWriteMode.REORDERED);

//  /** The number of writes in the batchedWrite. */
//  private int batchedWriteCount = 0;

  public AsyncC8dbClient(C8DB c8db) {
    this.c8db = c8db;
  }

  public final void cleanup() throws DBException {
//    if (INIT_COUNT.decrementAndGet() == 0) {
//      try {
//        c8dbClient.close();
//      } catch (final Exception e1) {
//        System.err.println("Could not close c8db connection pool: "
//            + e1.toString());
//        e1.printStackTrace();
//        return;
//      } finally {
//        c8dbClient = null;
//        database = null;
//      }
//    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */

  public final void init() throws DBException {
    // Create a new C8DB client object
    c8db = new C8DB.Builder()
        .host("api-peamouth-0b57f3c7-us-southeast.paas.macrometa.io", 443)
        .build();
  }

  public Status read(C8DB c8db) {
    try {

//      // Select the database
//      c8db.db("networktraffictable");

      // Get the collection
      C8Database db = c8db.db("Tu_TZ0W2cR92-sr1j-l7ACA", "networktraffictable");
      C8Collection collection = db.collection("tb");
      collection.create();
//      collection.truncate();
//      C8Collection collection = db.collection("networktraffictable");

      // Create a new query
//      String query = "FOR d IN " + table + " FILTER d._key == @key RETURN d";

      // Set the query parameters
//      Map<String, Object> bindVars = new MapBuilder().put("key", key).get();

      // Execute the query
      C8Cursor<BaseDocument> cursor = db.query(
          "FOR i IN networktraffictable RETURN i",
          BaseDocument.class
      );
      System.out.println(cursor);
      // Get the result
      if (cursor.hasNext()) {
        BaseDocument document = cursor.next();
        fillMap(result, document);
        return Status.OK;
      } else {
        return Status.NOT_FOUND;
      }
    } catch (Exception e) {
      System.err.println(e.getMessage());
      return Status.ERROR;
    }
  }


  public Status scan(String s, String s1, int i, Set<String> set, Vector<HashMap<String, ByteIterator>> vector) {
    return null;
  }


  public Status update(String s, String s1, Map<String, ByteIterator> map) {
    return null;
  }


  public Status insert(String s, String s1, Map<String, ByteIterator> map) {
    return null;
  }


  public Status delete(String s, String s1) {
    return null;
  }

  private static void fillMap(Map<String, ByteIterator> result, BaseDocument document) {
    for (Map.Entry<String, Object> entry : document.getProperties().entrySet()) {
      Object value = entry.getValue();
      if (value instanceof String) {
        result.put(entry.getKey(), new StringByteIterator((String) value));
      } else if (value instanceof Number) {
        result.put(entry.getKey(), new StringByteIterator(value.toString()));
      } else if (value instanceof byte[]) {
        result.put(entry.getKey(), new ByteArrayByteIterator((byte[]) value));
      }
    }
  }
}


//    final int count = INIT_COUNT.incrementAndGet();
//
//    synchronized (AsyncC8dbClient.class) {
//      final Properties props = getProperties();
//
//      if (c8dbClient != null) {
//        database = c8dbClient.getDatabase(databaseName);
//        return;
//      }
//
//      // Set insert batchsize, default 1 - to be YCSB-original equivalent
//      batchSize = Integer.parseInt(props.getProperty("c8db.batchsize", "1"));
//
//      // Set is inserts are done as upserts. Defaults to false.
//      useUpsert = Boolean.parseBoolean(
//          props.getProperty("c8db.upsert", "false"));
//
//      // Just use the standard connection format URL
//      // http://docs.mongodb.org/manual/reference/connection-string/
//      // to configure the client.
//      String url =
//          props
//              .getProperty("c8db.url", "c8db://api-peamouth-0b57f3c7.paas.macrometa.io/_fabric/_system/_api/collection/networktraffictable");
//      if (!url.startsWith("c8db://")) {
//        System.err.println("ERROR: Invalid URL: '" + url
//            + "'. Must be of the form "
//            + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?"
//            + "options'. See "
//            + "http://docs.mongodb.org/manual/reference/connection-string/.");
//        System.exit(1);
//      }
//
//      C8dbUri uri = new C8dbUri(url);
//
//      try {
//        databaseName = uri.getDatabase();
////        url = "https://api-peamouth-0b57f3c7.paas.macrometa.io/_fabric/_system/_api/collection/networktraffictable";
//        if ((databaseName == null) || databaseName.isEmpty()) {
//          // Default database is "ycsb" if database is not
//          // specified in URL
//          databaseName = "ycsb";
//        }
//
//        mongoClient = MongoFactory.createClient(uri);
//
//        MongoClientConfiguration config = mongoClient.getConfig();
//        if (!url.toLowerCase().contains("locktype=")) {
//          config.setLockType(LockType.LOW_LATENCY_SPIN); // assumed...
//        }
//
//        readPreference = config.getDefaultReadPreference();
//        writeConcern = config.getDefaultDurability();
//
//        database = c8dbClient.getDatabase(databaseName);
//
//        System.out.println("mongo connection created with " + url);
//      } catch (final Exception e1) {
//        System.err
//            .println("Could not initialize MongoDB connection pool for Loader: "
//                + e1.toString());
//        e1.printStackTrace();
//        return;
//      }
//    }
