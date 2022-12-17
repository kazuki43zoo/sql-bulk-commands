package com.example.tools;

import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;

import java.io.IOException;

class SqlBulkCommandsApplicationRunnerTests {

  private final SqlBulkCommandsApplicationRunner runner = new SqlBulkCommandsApplicationRunner();

  @Test
  void addingColumns1() throws IOException {
    String[] args = { "--command=adding-columns", "--files=a.sql", "--column-names=d",
        "--column-values='0'",
        "--dir=target/test-classes/data" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void addingColumns2() throws IOException {
    String[] args = { "--command=adding-columns", "--files=b.sql", "--column-names=d",
        "--column-values=#_valueMappings[foo][#a]",
        "--dir=target/test-classes/data", "--value-mapping-files=src/test/resources/value-mappings.yml",
        "--table-definition-files=src/test/resources/table-definitions.yml" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void addingColumns3() throws IOException {
    String[] args = { "--command=adding-columns", "--files=m.sql", "--column-names=d",
        "--column-values='0'",
        "--first",
        "--dir=target/test-classes/data" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void addingColumns4() throws IOException {
    String[] args = { "--command=adding-columns", "--files=n.sql", "--column-names=d",
        "--column-values='0'",
        "--after-by-name=b",
        "--dir=target/test-classes/data" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void addingColumns5() throws IOException {
    String[] args = { "--command=adding-columns", "--files=o.sql", "--column-names=d",
        "--column-values='0'",
        "--after-by-position=2",
        "--dir=target/test-classes/data" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void addingColumns6() throws IOException {
    String[] args = { "--command=adding-columns", "--files=p.sql", "--column-names=d",
        "--column-values='0'",
        "--after-by-position=3",
        "--dir=target/test-classes/data" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void addingColumnsWithIndexedRef() throws IOException {
    String[] args = { "--command=adding-columns", "--files=c.sql", "--column-names=d",
        "--column-values=#column1",
        "--dir=target/test-classes/data" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void updatingColumns() throws IOException {
    String[] args = { "--command=updating-columns", "--files=d.sql", "--column-names=a,b",
        "--column-values=#_valueMappings[foo][#a]?:#a,T(java.lang.Integer).parseInt(#b)*10",
        "--dir=target/test-classes/data", "--value-mapping-files=src/test/resources/value-mappings.yml",
        "--table-definition-files=src/test/resources/table-definitions.yml" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void updatingColumnsWithIndexes() throws IOException {
    String[] args = { "--command=updating-columns", "--files=e.sql", "--column-positions=1,2",
        "--column-values=#_valueMappings[foo][#column1]?:#column0,T(java.lang.Integer).parseInt(#column2)*10",
        "--dir=target/test-classes/data", "--value-mapping-files=src/test/resources/value-mappings.yml" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void deletingColumns() throws IOException {
    String[] args = { "--command=deleting-columns", "--files=h.sql", "--column-names=c",
        "--dir=target/test-classes/data",
        "--table-definition-files=src/test/resources/table-definitions.yml" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void deletingColumnsWithIndexes() throws IOException {
    String[] args = { "--command=deleting-columns", "--files=i.sql", "--column-positions=3",
        "--dir=target/test-classes/data" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void orderingColumns() throws IOException {
    String[] args = { "--command=ordering-columns", "--files=j.sql", "--column-names=c,a,b",
        "--dir=target/test-classes/data",
        "--table-definition-files=src/test/resources/table-definitions.yml" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void orderingColumnsWithColumnIndexes() throws IOException {
    String[] args = { "--command=ordering-columns", "--files=k.sql", "--column-positions=3,1,2",
        "--dir=target/test-classes/data" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void formatting() throws IOException {
    String[] args = { "--command=formatting", "--files=l.sql",
        "--dir=target/test-classes/data" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void generatingCsv() throws IOException {
    String[] args = { "--command=generating-csv", "--files=q.sql",
        "--dir=target/test-classes/data", "--column-names=g,h,i",
        "--table-definition-files=src/test/resources/table-definitions.yml" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void noArgs() throws IOException {
    String[] args = {};
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void h() throws IOException {
    String[] args = { "--h" };
    runner.run(new DefaultApplicationArguments(args));
  }

  @Test
  void help() throws IOException {
    String[] args = { "--help" };
    runner.run(new DefaultApplicationArguments(args));
  }
}
