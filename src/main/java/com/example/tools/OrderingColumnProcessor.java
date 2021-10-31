package com.example.tools;

import org.springframework.expression.EvaluationContext;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class OrderingColumnProcessor extends SqlBulkCommandsProcessorSupport {

  static final OrderingColumnProcessor INSTANCE = new OrderingColumnProcessor();

  private OrderingColumnProcessor() {
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
      if (valueColumns.size() != columnIndexes.size()) {
        logger.warn(
            "Skip ordering because column size not same with values and column-indexes. columns:{} column-indexes:{} sql-template:{} columns:{} values:{}",
            valueColumns.size(), columnIndexes.size(), sqlTemplate, columns, values);
        return null;
      }
      List<String> orderedColumnNames = new ArrayList<>();
      if (!headerColumns.isEmpty()) {
        for (int columnIndex : columnIndexes) {
          orderedColumnNames.add(headerColumns.get(columnIndex));
        }
      }
      List<String> orderedColumnValues = new ArrayList<>();
      for (int columnIndex : columnIndexes) {
        orderedColumnValues.add(valueColumns.get(columnIndex));
      }
      return formatSql(sqlTemplate, columns, orderedColumnNames, orderedColumnValues);
    } else {
      if (headerColumns.size() != columnNames.size()) {
        logger.warn(
            "Skip ordering because column size not same. before:{} after:{} before-columns:{} after-columns:{} sql-template:{} columns:{} values:{}",
            headerColumns.size(), columnNames.size(),
            headerColumns,
            columnNames, sqlTemplate, columns, values);
        return null;
      }
      if (!headerColumns.containsAll(columnNames)) {
        logger.warn(
            "Skip ordering because columns not same. before-columns:{} after-columns:{} sql-template:{} columns:{} values:{}",
            headerColumns,
            columnNames, sqlTemplate, columns, values);
        return null;
      }
      if (headerColumns.equals(columnNames)) {
        logger.info("Skip ordering because same ordering. sql-template:{} columns:{} values:{}", sqlTemplate, columns,
            values);
        return null;
      }
      List<Integer> orderedColumnIndexes = new ArrayList<>();
      for (String column : columnNames) {
        orderedColumnIndexes.add(headerColumns.indexOf(column));
      }
      List<String> orderedColumnValues = new ArrayList<>();
      for (Integer index : orderedColumnIndexes) {
        orderedColumnValues.add(valueColumns.get(index));
      }
      return formatSql(sqlTemplate, columns, columnNames, orderedColumnValues);
    }
  }

}
