package site.ycsb.db;

import com.c8db.C8DB;
import com.c8db.C8Database;
import com.c8db.Protocol;
import com.c8db.entity.BaseDocument;
import com.c8db.entity.CollectionEntity;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

import java.util.*;

public class C8dbTestAPI {
  @Test
  public void testRead() {

  }

  private static final String TEST_TABLE = "testTable";

  private static final String[] TEST_FIELDS = {"field1", "field2"};

  private static final String TEST_KEY_EXISTS = "key1";
  private static final String TEST_KEY_NOT_EXISTS = "key2";

  private static final String TEST_VALUE_1 = "value1";
  private static final int TEST_VALUE_2 = 123;
  private static final byte[] TEST_VALUE_3 = {0x01, 0x02, 0x03};

  private static C8DB c8db;
  private static AsyncC8dbClient testClient;

  @BeforeClass
  public static void setUp() throws Exception {
    // Create a new C8DB client object
    C8DB c8db = new C8DB.Builder()
        .host("api-peamouth-0b57f3c7-us-southeast.paas.macrometa.io", 443)
        .build();

//    c8db.createDatabase("myDatabase");
//    c8db.db("myDatabase").createCollection(TEST_TABLE);
    c8db.db();

    // Populate the collection with test data
    testClient = new AsyncC8dbClient(c8db);
//    testClient.insert(TEST_TABLE, TEST_KEY_EXISTS, createTestDocument(TEST_VALUE_1, TEST_VALUE_2, TEST_VALUE_3));
  }

//  @AfterClass
//  public static void tearDown() throws Exception {
//    // Delete the test database
////    c8db.dropDatabase("myDatabase");
//
//    // Close the C8DB client
//    c8db.shutdown();
//  }

  @Test
  public void testReadExistingDocument() {
    C8DB c8db = new C8DB.Builder()
        .useProtocol(Protocol.HTTP_JSON)
        .host("api-peamouth-0b57f3c7.paas.macrometa.io",8529)
        .build();
    Collection<CollectionEntity> infos = c8db.db("Tu_TZ0W2cR92-sr1j-l7ACA","_system").getCollections();
    // Test reading an existing document with all fields
    Map<String, ByteIterator> result = new HashMap<>();
    Status status = testClient.read(c8db);
    Assert.assertEquals(Status.OK, status);
    Assert.assertEquals(3, result.size());
    Assert.assertEquals(TEST_VALUE_1, result.get("field1").toString());
    Assert.assertEquals(TEST_VALUE_2, Integer.parseInt(result.get("field2").toString()));
    Assert.assertArrayEquals(TEST_VALUE_3, (byte[]) result.get("field3").toArray());

//    // Test reading an existing document with selected fields
//    result = new HashMap<>();
//    status = testClient.read(TEST_TABLE, TEST_KEY_EXISTS, new HashSet<>(Arrays.asList(TEST_FIELDS)), result);
//    Assert.assertEquals(Status.OK, status);
//    Assert.assertEquals(2, result.size());
//    Assert.assertEquals(TEST_VALUE_1, result.get("field1").toString());
//    Assert.assertEquals(TEST_VALUE_2, Integer.parseInt(result.get("field2").toString()));
  }

//  @Test
//  public void testReadNonExistingDocument() {
//    // Test reading a non-existing document
//    Map<String, ByteIterator> result = new HashMap<>();
//    Status status = testClient.read(TEST_TABLE, TEST_KEY_NOT_EXISTS, null, result);
//    Assert.assertEquals(Status.NOT_FOUND, status);
//    Assert.assertEquals(0, result.size());
//  }
//
//  private static BaseDocument createTestDocument(String field1, int field2, byte[] field3) {
//    BaseDocument document = new BaseDocument();
//    document.addAttribute("field1", field1);
//    document.addAttribute("field2", field2);
//    document.addAttribute("field3", field3);
//    return document;
//  }
}