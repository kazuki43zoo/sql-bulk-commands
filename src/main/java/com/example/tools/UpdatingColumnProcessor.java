package com.example.tools;

import org.springframework.expression.EvaluationContext;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class UpdatingColumnProcessor extends SqlBulkCommandsProcessorSupport {

  static final UpdatingColumnProcessor INSTANCE = new UpdatingColumnProcessor();

  private UpdatingColumnProcessor() {
    // NOP
  }

  @Override
  protected String generateSql(String tableName, String sqlTemplate, String columns, String values,
      List<String> columnNames, List<Integer> columnIndexes,
      List<String> columnValues, Map<String, Object> valueMappings, Map<String, Object> tableDefinitions) {
    List<String> headerColumns = getHeaderColumns(tableName, columns, tableDefinitions);
    List<String> valueColumns = commaDelimitedList(values).stream().map(String::trim)
        .collect(Collectors.toList());

    if (!columnIndexes.isEmpty()) {
      Map<String, Integer> headerIndexMap = new LinkedHashMap<>();
      for (int i = 0; i < valueColumns.size(); i++) {
        headerIndexMap.put("column" + (i + 1), i);
      }
      EvaluationContext context = createEvaluationContext(headerIndexMap, valueColumns, valueMappings);
      for (int i = 0; i < columnIndexes.size(); i++) {
        valueColumns.set(columnIndexes.get(i), eval(columnValues.get(i), context));
      }
    } else {
      if (headerColumns.containsAll(columnNames)) {
        Map<String, Integer> headerIndexMap = new LinkedHashMap<>();
        for (int i = 0; i < headerColumns.size(); i++) {
          headerIndexMap.put(headerColumns.get(i), i);
          headerIndexMap.put("column" + (i + 1), i);
        }
        Map<Integer, String> columnIndexValueMap = new LinkedHashMap<>();
        for (String column : columnNames) {
          columnIndexValueMap.put(headerColumns.indexOf(column), columnValues.get(columnIndexValueMap.size()));
        }
        EvaluationContext context = createEvaluationContext(headerIndexMap, valueColumns, valueMappings);
        for (Map.Entry<Integer, String> entry : columnIndexValueMap.entrySet()) {
          valueColumns.set(entry.getKey(), eval(entry.getValue(), context));
        }
      } else {
        logger.warn(
            "Skip setting because columns not exists. header-columns:{} target-columns:{} sql-template:{} columns:{} values:{}",
            headerColumns,
            columnNames, sqlTemplate, columns, values);
      }
    }
    return formatSql(sqlTemplate, columns, headerColumns, valueColumns);
  }

}
