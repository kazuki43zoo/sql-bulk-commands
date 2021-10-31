package com.example.tools;

import org.springframework.expression.EvaluationContext;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class DeletingColumnProcessor extends SqlBulkCommandsProcessorSupport {

  static final DeletingColumnProcessor INSTANCE = new DeletingColumnProcessor();

  private DeletingColumnProcessor() {
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

    if (!columnIndexes.isEmpty()) {
      List<String> validColumnNames = new ArrayList<>();
      for (int i = 0; i < headerColumns.size(); i++) {
        if (!columnIndexes.contains(i)) {
          validColumnNames.add(headerColumns.get(i));
        }
      }
      List<String> validColumnValues = new ArrayList<>();
      for (int i = 0; i < valueColumns.size(); i++) {
        if (!columnIndexes.contains(i)) {
          validColumnValues.add(valueColumns.get(i));
        }
      }
      return formatSql(sqlTemplate, columns, validColumnNames, validColumnValues);
    } else {
      Map<Integer, Boolean> containsColumnMap = new LinkedHashMap<>();
      for (String column : headerColumns) {
        containsColumnMap.put(containsColumnMap.size(), columnNames.contains(column));
      }
      List<String> validColumnNames = new ArrayList<>();
      for (Map.Entry<Integer, Boolean> entry : containsColumnMap.entrySet()) {
        if (!entry.getValue()) {
          validColumnNames.add(headerColumns.get(entry.getKey()));
        }
      }
      List<String> validColumnValues = new ArrayList<>();
      for (Map.Entry<Integer, Boolean> entry : containsColumnMap.entrySet()) {
        if (!entry.getValue()) {
          validColumnValues.add(valueColumns.get(entry.getKey()));
        }
      }
      return formatSql(sqlTemplate, columns, validColumnNames, validColumnValues);
    }
  }

}
