package com.example.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.YamlMapFactoryBean;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class SqlBulkCommandsApplicationRunner implements ApplicationRunner, ExitCodeGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlBulkCommandsApplicationRunner.class);

  private int exitCode;

  @Override
  public void run(ApplicationArguments args) throws IOException {
    if (args.getSourceArgs().length == 0 || args.containsOption("h") || args.containsOption("help")) {
      System.out.println();
      System.out.println("[Command arguments]");
      System.out.println("  --command");
      System.out.println("       adding-columns deleting-columns updating-columns ordering-columns formatting");
      System.out.println("  --dir");
      System.out.println("       target directory for apply command(can search target files on specified directory)");
      System.out.println("  --files");
      System.out.println("       target files for apply command(can filter that ending with specified file name)");
      System.out.println("  --column-names");
      System.out.println("       list of column name");
      System.out.println("  --column-positions");
      System.out.println("       list of column position(start with 1)");
      System.out.println("  --column-values");
      System.out.println(
          "       list of column value(can reference other column values using SpEL expression)");
      System.out.println("  --value-mapping-files");
      System.out.println("       mapping yaml files for value converting");
      System.out.println(
          "       can be accessed using an SpEL like as #_valueMappings[{value-name}][{value}] (e.g. --column-names=foo --column-values=#_valueMappings[foo][#foo]?:'0')");
      System.out.println("       e.g.) value mapping yaml file");
      System.out.println("       foo:");
      System.out.println("         \"10\": \"1\"");
      System.out.println("         \"20\": \"2\"");
      System.out.println("       bar:");
      System.out.println("         \"10\": \"2\"");
      System.out.println("         \"20\": \"1\"");
      System.out.println("  --table-definition-files");
      System.out.println(
          "       table definition yaml files for resolving column names for sql without column list(e.g. insert into t_users values ('123', 'Kazuki Shimizu', 'kazuki@test.com'))");
      System.out.println("       e.g.) table definition yaml file");
      System.out.println("       t_users:");
      System.out.println("         - id");
      System.out.println("         - name");
      System.out.println("         - mail");
      System.out.println("       t_transactions:");
      System.out.println("         - id");
      System.out.println("         - vendor_id");
      System.out.println("         - amount");
      System.out.println("  --h (--help)");
      System.out.println("       print help");
      System.out.println();
      System.out.println("[Exit Code]");
      System.out.println("  0 : There is no difference (normal end)");
      System.out.println("  1 : Was occurred an application error");
      System.out.println("  2 : Command arguments invalid");
      System.out.println();
      System.out.println("[Usage: adding-columns]");
      System.out.println("  Adding specified new column using column-names and column-values.");
      System.out.println(
          "  e.g.) --command=adding-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-names=b,c --column-values=1,#a");
      System.out.println("  ------------------------");
      System.out.println("  insert into xxxx (a) values('123');");
      System.out.println("  ------------------------");
      System.out.println("    ↓");
      System.out.println("  ------------------------");
      System.out.println("  insert into xxxx (a, b, c) values ('123', 1, '123');");
      System.out.println("  ------------------------");
      System.out.println();
      System.out.println("[Usage: deleting-columns]");
      System.out.println("  Deleting specified existing column using column-names(or column-positions).");
      System.out.println(
          "  e.g.) --command=deleting-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-names=b");
      System.out.println(
          "        --command=deleting-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-positions=2");
      System.out.println("  ------------------------");
      System.out.println("  insert into xxxx (a, b, c) values ('123', 1, '0');");
      System.out.println("  ------------------------");
      System.out.println("    ↓");
      System.out.println("  ------------------------");
      System.out.println("  insert into xxxx (a, c) values ('123', '0');");
      System.out.println("  ------------------------");
      System.out.println();
      System.out.println("[Usage: updating-columns]");
      System.out.println("  Updating value specified existing column using column-names(or column-positions) and column-values.");
      System.out.println(
          "  e.g.) --command=updating-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-names=b --column-values=NULL");
      System.out.println(
          "        --command=updating-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-positions=2 --column-values=NULL");
      System.out.println("  ------------------------");
      System.out.println("  insert into xxxx (a, b, c) values ('123', 1, '0');");
      System.out.println("  ------------------------");
      System.out.println("    ↓");
      System.out.println("  ------------------------");
      System.out.println("  insert into xxxx (a, b, c) values ('123', NULL, '0');");
      System.out.println("  ------------------------");
      System.out.println();
      System.out.println("[Usage: ordering-columns]");
      System.out.println("  Ordering column specified order using column-names(or column-positions).");
      System.out.println(
          "  e.g.) --command=ordering-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-names=c,a,b");
      System.out.println(
          "        --command=ordering-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-positions=3,1,2");
      System.out.println("  ------------------------");
      System.out.println("  insert into xxxx (a, b, c) values ('123', 1, '0');");
      System.out.println("  ------------------------");
      System.out.println("    ↓");
      System.out.println("  ------------------------");
      System.out.println("  insert into xxxx (c, a, b) values ('0', '123', 1);");
      System.out.println("  ------------------------");
      System.out.println();
      System.out.println("[Usage: formatting]");
      System.out.println("  Formatting sql to one line format and separate column list using ', '.");
      System.out.println(
          "  e.g.) --command=formatting --dir=src/test/resources/data --files=xxx.sql,yyy.sql");
      System.out.println("  ------------------------");
      System.out.println("  insert into xxxx\n     (a,b,c) values ('123',    1,    '0');");
      System.out.println("  ------------------------");
      System.out.println("    ↓");
      System.out.println("  ------------------------");
      System.out.println("  insert into xxxx (a, b, c) values ('123', 1, '0');");
      System.out.println("  ------------------------");
      System.out.println();
      return;
    }

    String command;
    if (args.containsOption("command")) {
      command = args.getOptionValues("command").stream().findFirst().orElse("");
    } else {
      this.exitCode = 2;
      LOGGER.warn(
          "'command' is required. valid-commands:[adding-columns, deleting-columns, updating-columns, ordering-columns, formatting]");
      return;
    }

    String dir;
    if (args.containsOption("dir")) {
      dir = args.getOptionValues("dir").stream().findFirst()
          .orElseThrow(() -> new IllegalArgumentException("'dir' value is required."));
    } else {
      this.exitCode = 2;
      LOGGER.warn("'dir' is required.");
      return;
    }

    List<String> files;
    if (args.containsOption("files")) {
      files = args.getOptionValues("files").stream().flatMap(x -> StringUtils.commaDelimitedListToSet(x).stream())
          .distinct().collect(
              Collectors.toList());
    } else {
      this.exitCode = 2;
      LOGGER.warn("'files' is required.");
      return;
    }

    List<String> columnNames = args.containsOption("column-names") ?
        args.getOptionValues("column-names").stream()
            .flatMap(x -> Stream.of(StringUtils.commaDelimitedListToStringArray(x))).collect(Collectors.toList()) :
        Collections.emptyList();

    List<Integer> columnPositions = args.containsOption("column-positions") ?
        args.getOptionValues("column-positions").stream()
            .flatMap(x -> Stream.of(StringUtils.commaDelimitedListToStringArray(x))).map(Integer::valueOf)
            .collect(Collectors.toList()) :
        Collections.emptyList();

    List<String> columnValues = args.containsOption("column-values") ?
        args.getOptionValues("column-values").stream()
            .flatMap(x -> Stream.of(StringUtils.commaDelimitedListToStringArray(x))).collect(Collectors.toList()) :
        Collections.emptyList();

    Charset encoding = args.containsOption("encoding") ?
        Charset.forName(args.getOptionValues("encoding").stream().findFirst().orElse(StandardCharsets.UTF_8.name())) :
        StandardCharsets.UTF_8;

    final Map<String, Object> valueMappings;
    if (args.containsOption("value-mapping-files")) {
      YamlMapFactoryBean yamlMapFactoryBean = new YamlMapFactoryBean();
      yamlMapFactoryBean.setResources(
          args.getOptionValues("value-mapping-files").stream().map(FileSystemResource::new).toArray(
              Resource[]::new));
      valueMappings = yamlMapFactoryBean.getObject();
    } else {
      valueMappings = Collections.emptyMap();
    }

    final Map<String, Object> tableDefinitions;
    if (args.containsOption("table-definition-files")) {
      YamlMapFactoryBean yamlMapFactoryBean = new YamlMapFactoryBean();
      yamlMapFactoryBean.setResources(
          args.getOptionValues("table-definition-files").stream().map(FileSystemResource::new).toArray(
              Resource[]::new));
      tableDefinitions = yamlMapFactoryBean.getObject();
    } else {
      tableDefinitions = Collections.emptyMap();
    }

    LOGGER.info(
        "Start. command:{} dir:{} files:{} encoding:{} column-names:{} column-positions:{} column-values:{} value-mappings:{} table-definitions:{}",
        command, dir, files, encoding, columnNames, columnPositions, columnValues, valueMappings, tableDefinitions);

    try {
      Files.walk(Paths.get(dir))
          .filter(Files::isRegularFile)
          .filter(file -> files.stream().anyMatch(x -> file.toString().replace('\\', '/').endsWith(x)))
          .sorted().forEach(
              file -> execute(command, columnNames, columnPositions, columnValues, file, encoding, valueMappings,
                  tableDefinitions));
    }
    catch (IllegalArgumentException e) {
      this.exitCode = 2;
      LOGGER.warn(e.getMessage());
    }

    LOGGER.info("End.");

  }

  private void execute(String command, List<String> columnNames, List<Integer> columnPositions,
      List<String> columnValues,
      Path file, Charset encoding,
      Map<String, Object> valueMappings, Map<String, Object> tableDefinitions) {
    LOGGER.info("processing file:{}", file);
    switch (command) {
    case "adding-columns":
      if (columnNames.isEmpty()) {
        throw new IllegalArgumentException("'column-names' is required.");
      }
      if (columnNames.size() != columnValues.size()) {
        throw new IllegalArgumentException("'column-names' and 'column-values' should be same size.");
      }
      AddingColumnProcessor.INSTANCE.execute(columnNames, columnPositions, columnValues, file, encoding, valueMappings,
          tableDefinitions);
      break;
    case "deleting-columns":
      if (columnNames.isEmpty() && columnPositions.isEmpty()) {
        throw new IllegalArgumentException("'column-names' or 'column-positions' is required.");
      }
      DeletingColumnProcessor.INSTANCE.execute(columnNames, columnPositions, columnValues, file, encoding,
          valueMappings,
          tableDefinitions);
      break;
    case "updating-columns":
      if (columnNames.isEmpty() && columnPositions.isEmpty()) {
        throw new IllegalArgumentException("'column-names' or 'column-positions' is required.");
      }
      if (columnNames.size() != columnValues.size() && columnPositions.size() != columnValues.size()) {
        throw new IllegalArgumentException(
            "'column-names' (or 'column-indexes') and 'column-values' should be same size.");
      }
      UpdatingColumnProcessor.INSTANCE.execute(columnNames, columnPositions, columnValues, file, encoding,
          valueMappings,
          tableDefinitions);
      break;
    case "ordering-columns":
      if (columnNames.isEmpty() && columnPositions.isEmpty()) {
        throw new IllegalArgumentException("'column-names' or 'column-positions' is required.");
      }
      OrderingColumnProcessor.INSTANCE.execute(columnNames, columnPositions, columnValues, file, encoding,
          valueMappings,
          tableDefinitions);
      break;
    case "formatting":
      FormattingProcessor.INSTANCE.execute(columnNames, columnPositions, columnValues, file, encoding, valueMappings,
          tableDefinitions);
      break;
    default:
      throw new IllegalArgumentException(String.format("'%s' command not support. valid-commands:%s", command,
          "[adding-columns, deleting-columns, updating-columns, ordering-columns, formatting]"));
    }

  }

  @Override
  public int getExitCode() {
    return exitCode;
  }

}
