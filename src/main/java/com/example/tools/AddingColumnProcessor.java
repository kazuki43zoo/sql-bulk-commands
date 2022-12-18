package com.example.tools;

import org.springframework.expression.EvaluationContext;
import org.springframework.util.StringUtils;

import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AddingColumnProcessor extends SqlBulkCommandsProcessorSupport {

  static final AddingColumnProcessor INSTANCE = new AddingColumnProcessor();
  private final ThreadLocal<String> currentMethod = new ThreadLocal<>();
  private final ThreadLocal<String> currentTarget = new ThreadLocal<>();

  private AddingColumnProcessor() {
    // NOP
  }
  
  void execute(String method, String target,List<String> columnNames, List<Integer> columnPositions, List<String> columnValues, Path file,
      Charset encoding, Map<String, Object> valueMappings, Map<String, Object> tableDefinitions) {
    currentMethod.set(method);
    currentTarget.set(target);
    try {
      super.execute(columnNames, columnPositions, columnValues, file, encoding, valueMappings, tableDefinitions);
    } finally {
      currentMethod.remove();
      currentTarget.remove();
    }
  }

  @Override
  protected String generateSql(String tableName, String sqlTemplate, String columns, String values,
      List<String> columnNames, List<Integer> columnIndexes,
      List<String> columnValues, Map<String, Object> valueMappings, Map<String, Object> tableDefinitions) {
    List<String> headerColumns = getHeaderColumns(tableName, columns, tableDefinitions);
    List<String> valueColumns = commaDelimitedList(values).stream().map(String::trim)
        .collect(Collectors.toList());

    String method = currentMethod.get();
    String target = currentTarget.get();

    if ("after-by-name".equalsIgnoreCase(method) && (headerColumns.isEmpty() || Stream.of(target, target.toLowerCase(),
            target.toUpperCase()).map(x -> headerColumns.indexOf(target))
        .noneMatch(x -> x != -1))) {
      throw new IllegalArgumentException(
          "Cannot use the 'after-by-name' option because no header names in sql or no provide table definition.");
    }

    Map<String, Integer> headerIndexMap = new LinkedHashMap<>();
    if (headerColumns.isEmpty()) {
      for (int i = 0; i < valueColumns.size(); i++) {
        headerIndexMap.put("column" + (i + 1), i);
      }
    } else {
      for (int i = 0; i < headerColumns.size(); i++) {
        headerIndexMap.put(headerColumns.get(i), i);
        headerIndexMap.put("column" + (i + 1), i);
      }
    }

    int addingStartIndex;
    if ("first".equalsIgnoreCase(method)) {
      addingStartIndex = 0;
    } else if ("after-by-name".equalsIgnoreCase(method)) {
      addingStartIndex =
          Stream.of(target, target.toLowerCase(), target.toUpperCase()).map(x -> headerColumns.indexOf(target))
              .filter(x -> x != -1).findAny().orElse(0) + 1;
    } else if ("after-by-position".equalsIgnoreCase(method)) {
      addingStartIndex = headerColumns.isEmpty() ? 0 : Integer.parseInt(target);
    } else {
      addingStartIndex = headerColumns.size();
    }

    Map<Integer, Boolean> containsColumnMap = new LinkedHashMap<>();
    int baseHeaderColumnSize = headerColumns.size();
    for (String columnName : columnNames) {
      boolean contains = headerColumns.contains(columnName);
      containsColumnMap.put(containsColumnMap.size(), contains);
      if (!contains) {
        headerColumns.add(addingStartIndex + (headerColumns.size() - baseHeaderColumnSize), columnName);
      }
    }
    List<String> validColumnValues = new ArrayList<>();
    for (Map.Entry<Integer, Boolean> entry : containsColumnMap.entrySet()) {
      if (!entry.getValue()) {
        validColumnValues.add(columnValues.get(entry.getKey()));
      }
    }
    EvaluationContext context = createEvaluationContext(headerIndexMap, valueColumns, valueMappings);
    if ("last".equalsIgnoreCase(method)) {
      addingStartIndex = valueColumns.size();
    } else if ("after-by-position".equalsIgnoreCase(method)) {
      addingStartIndex = Integer.parseInt(target);
    }
    int baseValueColumnSize = valueColumns.size();
    for (String value : validColumnValues) {
      valueColumns.add(addingStartIndex + (valueColumns.size() - baseValueColumnSize), eval(value, context));
    }
    return formatSql(sqlTemplate, columns, headerColumns, valueColumns);
  }

}
