package com.example.tools;

import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FormattingProcessor extends SqlBulkCommandsProcessorSupport {

  static final FormattingProcessor INSTANCE = new FormattingProcessor();

  private FormattingProcessor() {
    // NOP
  }

  @Override
  protected String generateSql(String tableName, String sqlTemplate, String columns, String values,
      List<String> columnNames, List<Integer> columnIndexes,
      List<String> columnValues, Map<String, Object> valueMappings, Map<String, Object> tableDefinitions) {
    List<String> headerColumns = getHeaderColumns(tableName, columns, tableDefinitions);
    List<String> valueColumns = commaDelimitedList(values).stream().map(String::trim)
        .collect(Collectors.toList());
    return formatSql(sqlTemplate, columns, headerColumns, valueColumns);
  }

}
