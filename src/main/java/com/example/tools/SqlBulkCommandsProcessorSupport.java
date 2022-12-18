package com.example.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

abstract class SqlBulkCommandsProcessorSupport {

  // for "insert into xxxx values (....);"
  protected static final Pattern VALUES_PATTERN = Pattern.compile("(^.+values *\\()(.+)(\\);$)",
      Pattern.CASE_INSENSITIVE);

  // for "insert into xxxx (...) values (....);"
  protected static final Pattern COLUMNS_VALUES_PATTERN = Pattern.compile("(^.+\\()(.+)(\\) +values *\\()(.+)(\\);$)",
      Pattern.CASE_INSENSITIVE);

  private static final ExpressionParser EXPRESSION_PARSER = new SpelExpressionParser();

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  void execute(List<String> columnNames, List<Integer> columnPositions, List<String> columnValues, Path file,
      Charset encoding,
      Map<String, Object> valueMappings, Map<String, Object> tableDefinitions) {
    List<Integer> columnIndexes = columnPositions.stream().map(x -> x - 1).collect(Collectors.toList());
    try {
      List<String> lines = Files.readAllLines(file, encoding);
      if (lines.isEmpty()) {
        logger.warn("Skip adding because file is empty. file:{}", file);
        return;
      }

      List<String> formattedLines = toFormattedLines(lines);

      List<String> saveLines = new ArrayList<>();
      for (String line : formattedLines) {
        String loweredLine = line.toLowerCase();
        if (loweredLine.isEmpty() || loweredLine.startsWith("--") || (loweredLine.startsWith("/*")
            && loweredLine.endsWith("*/"))) {
          // empty line and comment line
          saveLines.add(line);
        } else if (loweredLine.startsWith("insert")) {
          loweredLine = loweredLine.replaceAll("insert +into +", "");
          String tableName = loweredLine.substring(0, loweredLine.indexOf(" "));
          loweredLine = loweredLine.replaceFirst(tableName, "").trim();
          if (loweredLine.startsWith("values")) {
            Matcher matcher = VALUES_PATTERN.matcher(line);
            if (matcher.find()) {
              String generatedSql = generateSql(tableName, matcher.group(1) + "{0}" + matcher.group(3), null,
                  matcher.group(2),
                  columnNames, columnIndexes,
                  columnValues, valueMappings, tableDefinitions);
              saveLines.add(generatedSql == null ? line : generatedSql);
            } else {
              throw new UnsupportedOperationException("Skip operation because does not sql format. sql:" + line);
            }
          } else {
            Matcher matcher = COLUMNS_VALUES_PATTERN.matcher(line);
            if (matcher.find()) {
              String generatedSql = generateSql(tableName,
                  matcher.group(1) + "{0}" + matcher.group(3) + "{1}" + matcher.group(5),
                  matcher.group(2), matcher.group(4),
                  columnNames, columnIndexes, columnValues, valueMappings, tableDefinitions);
              saveLines.add(generatedSql == null ? line : generatedSql);
            } else {
              throw new UnsupportedOperationException("Skip operation because does not sql format. sql:" + line);
            }
          }
        } else {
          saveLines.add(line);
        }
      }

      Files.write(file, saveLines, encoding);
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected List<String> toFormattedLines(List<String> lines) {
    List<String> formattedLines = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    for (String line : lines) {
      String trimmedLine = line.trim();
      sb.append(trimmedLine);
      if (trimmedLine.isEmpty() || trimmedLine.startsWith("--") || (trimmedLine.startsWith("/*")
          && trimmedLine.endsWith("*/"))) {
        formattedLines.add(trimmedLine);
        sb.setLength(0);
      } else if (trimmedLine.endsWith(";")) {
        formattedLines.add(sb.toString().replaceAll(";+$", ";"));
        sb.setLength(0);
      } else {
        sb.append(" ");
      }
    }
    return formattedLines;
  }

  protected String generateSql(String tableName, String sqlTemplate, String columns, String values,
      List<String> columnNames, List<Integer> columnIndexes,
      List<String> columnValues, Map<String, Object> valueMappings, Map<String, Object> tableDefinitions) {
    return null;
  }

  @SuppressWarnings("unchecked")
  protected List<String> getHeaderColumns(String tableName, String columns, Map<String, Object> tableDefinitions) {
    List<String> headerColumns;
    if (columns == null) {
      headerColumns = tableDefinitions.containsKey(tableName) ?
          new ArrayList<>((Collection<String>) tableDefinitions.get(tableName)) :
          new ArrayList<>();
    } else {
      headerColumns =
          Arrays.stream(StringUtils.commaDelimitedListToStringArray(columns)).map(String::trim).collect(
              Collectors.toList());
    }
    return headerColumns;
  }

  protected List<String> commaDelimitedList(String line) {
    return Arrays.stream(StringUtils.commaDelimitedListToStringArray(escapeComma(line)))
        .map(x -> x.contains("__COMMA__") ? x.replaceAll("__COMMA__", ",") : x)
        .collect(Collectors.toList());
  }

  private String escapeComma(String line) {
    Map<Character, Character> enclosingMap = new HashMap<>();
    enclosingMap.put('(', ')');
    enclosingMap.put('{', '}');
    enclosingMap.put('[', ']');
    if (line.contains("(") || line.contains("{") || line.contains("[")) {
      StringBuilder sb = new StringBuilder();
      List<Character> stack = new ArrayList<>();
      for (char c : line.toCharArray()) {
        if (enclosingMap.containsKey(c)) {
          sb.append(c);
          stack.add(enclosingMap.get(c));
        } else if (enclosingMap.containsValue(c)) {
          sb.append(c);
          if (stack.contains(c)) {
            stack.remove(stack.indexOf(c));
          }
        } else if (c == ',') {
          if (stack.isEmpty()) {
            sb.append(c);
          } else {
            sb.append("__COMMA__");
          }
        } else {
          sb.append(c);
        }
      }
      return sb.toString();
    }
    return line;
  }

  protected EvaluationContext createEvaluationContext(Map<String, Integer> headerIndexMap, List<String> valueColumns,
      Map<String, Object> valueMappings) {
    StandardEvaluationContext context = new StandardEvaluationContext();
    context.setVariable("_valueMappings", valueMappings);
    headerIndexMap.forEach(
        (name, index) -> context.setVariable(name,
            valueColumns.get(index).replaceAll("^'", "").replaceAll("'$", "")));

    return context;
  }

  protected String eval(String expressionValue, EvaluationContext context) {
    Expression expression = EXPRESSION_PARSER.parseExpression(expressionValue);
    Object evalValue = expression.getValue(context);
    if (evalValue instanceof String) {
      evalValue = "'" + evalValue + "'";
    } else if (evalValue == null) {
      if (expressionValue.equalsIgnoreCase("null")) {
        evalValue = expressionValue;
      }
    }
    return Objects.toString(evalValue);
  }

  protected String formatSql(String sqlTemplate, String columns, List<String> headerColumns,
      List<String> valueColumns) {
    return MessageFormat.format(sqlTemplate,
        columns == null ?
            new Object[] { String.join(",", valueColumns) } :
            new Object[] { String.join(",", headerColumns), String.join(",", valueColumns) });
  }
}