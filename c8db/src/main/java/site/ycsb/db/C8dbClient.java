package site.ycsb.db;

import com.c8db.C8Cursor;
import com.c8db.C8Database;
import com.c8db.C8Iterator;
import com.c8db.entity.CollectionEntity;
import com.c8db.internal.C8CollectionImpl;
import com.c8db.internal.C8Context;

import java.io.Closeable;
import java.util.List;

public interface C8dbClient extends Closeable {
  C8dbClient asSerializedClient();

//  C8Context getConfig();

  C8Database getDatabase(String var1);

  List<String> listDatabaseNames();

  /** @deprecated */
  @Deprecated
  List<String> listDatabases();

//  C8Iterator<CollectionEntity> restart(CollectionType var1) throws IllegalArgumentException;
//
//  C8Cursor<Document> restart(LambdaCallback<Document> var1, DocumentAssignable var2) throws IllegalArgumentException;
//
//  C8Cursor<Document> restart(StreamCallback<Document> var1, DocumentAssignable var2) throws IllegalArgumentException;
}
