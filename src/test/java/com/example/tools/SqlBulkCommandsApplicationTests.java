package com.example.tools;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//@SpringBootTest
class SqlBulkCommandsApplicationTests {

  @Test
  void contextLoads() {
    Pattern p1 = Pattern.compile("(.*\\()(.*)(\\).*)");
    Pattern p2 = Pattern.compile("(.*\\()(.*)(\\).*\\()(.*)(\\).*)");

    //    String sql = "insert into xxxx values ('aaaa',1,null);";
    String sql = "insert into xxxx (aaa, bbb, ccc) values ('aaaa',1,null);";
    String loweredSql = sql.toLowerCase();
    if (loweredSql.startsWith("insert")) {
      loweredSql = loweredSql.replaceAll("insert +into +", "");
      String tableName = loweredSql.substring(0, loweredSql.indexOf(" "));
      System.out.println(tableName);
      loweredSql = loweredSql.replaceFirst(tableName, "").trim();
      if (loweredSql.startsWith("values")) {
        Matcher m = p1.matcher(sql);
        if (m.find()) {
          System.out.println(m.group(1));
          System.out.println(m.group(2));
          System.out.println(m.group(3));
        }
      } else {
        Matcher m = p2.matcher(sql);
        if (m.find()) {
          System.out.println(m.group(1));
          System.out.println(m.group(2));
          System.out.println(m.group(3));
          System.out.println(m.group(4));
          System.out.println(m.group(5));
        }
      }
    }
  }

}
