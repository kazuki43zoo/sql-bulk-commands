package com.example.tools;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.transform.ExtractorLineAggregator;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class GeneratingCsvProcessor extends SqlBulkCommandsProcessorSupport {

  static final GeneratingCsvProcessor INSTANCE = new GeneratingCsvProcessor();

  private GeneratingCsvProcessor() {
    // NOP
  }

  void execute(List<String> columnNames, Path file, Charset encoding, Map<String, Object> tableDefinitions,
      String delimiter, Boolean ignoreEscapedEnclosure) {
    try {
      List<String> lines = Files.readAllLines(file, encoding);
      if (lines.isEmpty()) {
        logger.warn("Skip adding because file is empty. file:{}", file);
        return;
      }

      List<String> formattedLines = toFormattedLines(lines);

      Map<String, List<String>> columnsMap = new HashMap<>();
      Map<String, List<String[]>> valuesLinesMap = new HashMap<>();
      Map<String, String> truncateTableSql = new HashMap<>();

      for (String line : formattedLines) {
        String loweredLine = line.toLowerCase();
        if (loweredLine.startsWith("truncate")) {
          String tableName = loweredLine.substring(loweredLine.lastIndexOf(" "), loweredLine.indexOf(";")).trim();
          truncateTableSql.put(tableName, loweredLine);
          continue;
        }
        if (!loweredLine.startsWith("insert")) {
          continue;
        }
        loweredLine = loweredLine.replaceAll("insert +into +", "");
        String tableName = loweredLine.substring(0, loweredLine.indexOf(" "));
        loweredLine = loweredLine.replaceFirst(tableName, "").trim();
        List<String> columns;
        List<String> values;
        if (loweredLine.startsWith("values")) {
          Matcher matcher = VALUES_PATTERN.matcher(line);
          if (matcher.find()) {
            columns = Collections.emptyList();
            values = commaDelimitedList(matcher.group(2)).stream()
                .map(String::trim)
                .map(x -> x.equals("null") ? "NULL" : x)
                .map(x -> x.replaceAll("^'|'$|^\"|\"$", ""))
                .collect(Collectors.toList());
          } else {
            throw new UnsupportedOperationException("Skip operation because does not sql format. sql:" + line);
          }
        } else {
          Matcher matcher = COLUMNS_VALUES_PATTERN.matcher(line);
          if (matcher.find()) {
            columns = getHeaderColumns(tableName, matcher.group(2), Collections.emptyMap());
            values = commaDelimitedList(matcher.group(4)).stream()
                .map(String::trim)
                .map(x -> x.equals("null") ? "NULL" : x)
                .map(x -> x.replaceAll("^'|'$|^\"|\"$", ""))
                .collect(Collectors.toList());
          } else {
            throw new UnsupportedOperationException("Skip operation because does not sql format. sql:" + line);
          }
        }
        if (!columns.isEmpty()) {
          columnsMap.putIfAbsent(tableName, columns);
        }
        valuesLinesMap.computeIfAbsent(tableName, x -> new ArrayList<>()).add(values.toArray(new String[0]));
      }
      for (String tableName : valuesLinesMap.keySet()) {
        List<String> headerColumns = Optional.ofNullable(columnsMap.get(tableName))
            .orElseGet(() -> getHeaderColumns(tableName, null, tableDefinitions));
        if (headerColumns.isEmpty()) {
          headerColumns.addAll(columnNames);
        }
        List<String[]> valuesLines = valuesLinesMap.get(tableName);
        valuesLines.add(0, headerColumns.toArray(new String[0]));
        Path csvFile = file.getParent().resolve(tableName + ".csv");
        logger.info("Generating csv file. tableName:{} file:{}", tableName, csvFile);
        writeLines(valuesLines, csvFile, encoding, delimiter, ignoreEscapedEnclosure);
      }
      for (Map.Entry<String, String> entry : truncateTableSql.entrySet()) {
        Files.write(file.getParent().resolve("truncate_" + entry.getKey() + ".sql"),
            Collections.singleton(entry.getValue()), encoding);
      }
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void writeLines(List<String[]> lines, Path file, Charset encoding, String delimiter,
      Boolean ignoreEscapedEnclosure) throws Exception {
    EnclosableDelimitedLineAggregator<String[]> aggregator = new EnclosableDelimitedLineAggregator<>();
    aggregator.setIgnoreEscapedEnclosure(Optional.ofNullable(ignoreEscapedEnclosure).orElse(false));
    aggregator.setDelimiter(Optional.ofNullable(delimiter).orElse(",").toCharArray()[0]);
    aggregator.afterPropertiesSet();
    ItemStreamWriter<String[]> itemWriter = new FlatFileItemWriterBuilder<String[]>()
        .lineAggregator(aggregator)
        .encoding(encoding.name())
        .resource(new FileSystemResource(file.toFile()))
        .name("default")
        .shouldDeleteIfEmpty(false)
        .transactional(false)
        .build();
    itemWriter.open(new ExecutionContext());
    try {
      itemWriter.write(lines);
    }
    finally {
      itemWriter.close();
    }
  }

  private static class EnclosableDelimitedLineAggregator<T> extends ExtractorLineAggregator<T> implements
      InitializingBean {
    protected boolean ignoreEscapedEnclosure = false;

    private final String enclosure = "\"";

    private final String escapedEnclosure;

    private String delimiter;

    public EnclosableDelimitedLineAggregator() {
      this.escapedEnclosure = this.enclosure + this.enclosure;
      this.delimiter = ",";
    }

    public void setIgnoreEscapedEnclosure(boolean ignoreEscapedEnclosure) {
      this.ignoreEscapedEnclosure = ignoreEscapedEnclosure;
    }

    public void setDelimiter(char delimiter) {
      this.delimiter = String.valueOf(delimiter);
    }

    public void afterPropertiesSet() {
      if (this.enclosure.equals(this.delimiter)) {
        throw new IllegalStateException(
            "the delimiter and enclosure must be different. [value:" + this.enclosure + "]");
      }
    }

    protected String doAggregate(Object[] fields) {
      return Arrays.stream(fields)
          .map(Object::toString)
          .map((field) -> this.hasTargetChar(field) ? this.encloseAndEscape(field) : field)
          .collect(Collectors.joining(this.delimiter));
    }

    private boolean hasTargetChar(String field) {
      return field.contains(this.delimiter) || field.contains(this.enclosure) || this.containsCrlf(field);
    }

    private boolean containsCrlf(String field) {
      return field.contains("\r") || field.contains("\n");
    }

    private String encloseAndEscape(String field) {
      return this.enclosure + (ignoreEscapedEnclosure ? field : field.replace(this.enclosure, this.escapedEnclosure))
          + this.enclosure;
    }
  }

}
