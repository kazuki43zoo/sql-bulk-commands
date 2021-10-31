package com.example.tools;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AddingColumnProcessor extends SqlBulkCommandsProcessorSupport {

  static final AddingColumnProcessor INSTANCE = new AddingColumnProcessor();

  private AddingColumnProcessor() {
    // NOP
  }

  @Override
  protected String generateSql(String tableName, String sqlTemplate, String columns, String values,
      List<String> columnNames, List<Integer> columnIndexes,
      List<String> columnValues, Map<String, Object> valueMappings, Map<String, Object> tableDefinitions) {
    List<String> headerColumns = getHeaderColumns(tableName, columns, tableDefinitions);
    List<String> valueColumns = Arrays.stream(StringUtils.commaDelimitedListToStringArray(values)).map(String::trim)
        .collect(
            Collectors.toList());
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
    Map<Integer, Boolean> containsColumnMap = new LinkedHashMap<>();
    for (String columnName : columnNames) {
      boolean contains = headerColumns.contains(columnName);
      containsColumnMap.put(containsColumnMap.size(), contains);
      if (!contains) {
        headerColumns.add(columnName);
      }
    }
    List<String> validColumnValues = new ArrayList<>();
    for (Map.Entry<Integer, Boolean> entry : containsColumnMap.entrySet()) {
      if (!entry.getValue()) {
        validColumnValues.add(columnValues.get(entry.getKey()));
      }
    }
    EvaluationContext context = createEvaluationContext(headerIndexMap, valueColumns, valueMappings);
    for (String value : validColumnValues) {
      valueColumns.add(eval(value, context));
    }
    return formatSql(sqlTemplate, columns, headerColumns, valueColumns);
  }

}
