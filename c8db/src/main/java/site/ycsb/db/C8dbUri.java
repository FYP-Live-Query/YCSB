package site.ycsb.db;

import java.util.List;
import java.util.Locale;

public class C8dbUri  {
  public static final String C8DB_URI_PREFIX = "c8db://";
  private final String myDatabase;
//  private final List<String> myHosts;
//  private final String myOptions;
//  private final String myOriginalText;
//  private final String myPassword;
//  private final String myUserName;
//  private final boolean myUseSsl;

  public C8dbUri(String var1) {

    if (isUri(var1)) {
      int index = var1.indexOf("/collection/");
      if (index != -1) {
        this.myDatabase = var1.substring(index + 12);
      } else {
        this.myDatabase = "networktraffictable";
      }
    } else {
      this.myDatabase = "networktraffictable";
    }
  }

  public static boolean isUri(String var0) {
    String var1 = var0.toLowerCase(Locale.US);
    return var1.startsWith("c8db://");
  }

  public String getDatabase() {
    return this.myDatabase;
  }
}
