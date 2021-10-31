# sql-bulk-commands

Bulk operation utilities for sql file.

## Features

Support following features.

* Adding columns by specified expression(fixed value or dynamic value) at the end
* Deleting columns
* Updating columns by specified expression(fixed value or dynamic value)
* Ordering columns
* Formatting sql to one line format and separate column list using `', '`
* Supports column position based operations for sql without column list such as `"insert into xxxx values('123', NULL, '0');"`
* Supports position based column value reference (variable name format: `column{position}`) such as `--column-values=#column1`

> **NOTE:**
>
> Supported sql type is "INSERT".

## Related libraries document

* [SpEL provided by Spring Framework](https://docs.spring.io/spring-framework/docs/current/reference/html/core.html#expressions)

## How to specify target files

Search files that matches conditions specified by `--dir` and `--files`.

* You need to specify a base directory using the `--dir`
* You need to specify a target file path suffix using the `--files`

## How to run

### Using Spring Boot Maven Plugin

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=""
```

```
[INFO] Scanning for projects...
[INFO] 
[INFO] -------------------< com.example:sql-bulk-commands >--------------------
[INFO] Building sql-bulk-commands 0.0.1-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO] 
[INFO] >>> spring-boot-maven-plugin:2.5.6:run (default-cli) > test-compile @ sql-bulk-commands >>>
[INFO] 
[INFO] --- maven-resources-plugin:3.2.0:resources (default-resources) @ sql-bulk-commands ---
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] Using 'UTF-8' encoding to copy filtered properties files.
[INFO] Copying 1 resource
[INFO] Copying 0 resource
[INFO] 
[INFO] --- maven-compiler-plugin:3.8.1:compile (default-compile) @ sql-bulk-commands ---
[INFO] Nothing to compile - all classes are up to date
[INFO] 
[INFO] --- maven-resources-plugin:3.2.0:testResources (default-testResources) @ sql-bulk-commands ---
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] Using 'UTF-8' encoding to copy filtered properties files.
[INFO] Copying 3 resources
[INFO] 
[INFO] --- maven-compiler-plugin:3.8.1:testCompile (default-testCompile) @ sql-bulk-commands ---
[INFO] Nothing to compile - all classes are up to date
[INFO] 
[INFO] <<< spring-boot-maven-plugin:2.5.6:run (default-cli) < test-compile @ sql-bulk-commands <<<
[INFO] 
[INFO] 
[INFO] --- spring-boot-maven-plugin:2.5.6:run (default-cli) @ sql-bulk-commands ---
[INFO] Attaching agents: []

[Command arguments]
  --command
       adding-columns deleting-columns updating-columns ordering-columns formatting
  --dir
       target directory for apply command(can search target files on specified directory)
  --files
       target files for apply command(can filter that ending with specified file name)
  --column-names
       list of column name
  --column-positions
       list of column position(start with 1)
  --column-values
       list of column value(can reference other column values using SpEL expression)
  --value-mapping-files
       mapping yaml files for value converting
       can be accessed using an SpEL like as #_valueMappings[{value-name}][{value}] (e.g. --column-names=foo --column-values=#_valueMappings[foo][#foo]?:'0')
       e.g.) value mapping yaml file
       foo:
         "10": "1"
         "20": "2"
       bar:
         "10": "2"
         "20": "1"
  --table-definition-files
       table definition yaml files for resolving column names for sql without column list(e.g. insert into t_users values ('123', 'Kazuki Shimizu', 'kazuki@test.com'))
       e.g.) table definition yaml file
       t_users:
         - id
         - name
         - mail
       t_transactions:
         - id
         - vendor_id
         - amount
  --h (--help)
       print help

[Exit Code]
  0 : There is no difference (normal end)
  1 : Was occurred an application error
  2 : Command arguments invalid

[Usage: adding-columns]
  Adding specified new column using column-names and column-values.
  e.g.) --command=adding-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-names=b,c --column-values=1,#a
  ------------------------
  insert into xxxx (a) values('123');
  ------------------------
    ↓
  ------------------------
  insert into xxxx (a, b, c) values ('123', 1, '123');
  ------------------------

[Usage: deleting-columns]
  Deleting specified existing column using column-names(or column-positions).
  e.g.) --command=deleting-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-names=b
        --command=deleting-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-positions=2
  ------------------------
  insert into xxxx (a, b, c) values ('123', 1, '0');
  ------------------------
    ↓
  ------------------------
  insert into xxxx (a, c) values ('123', '0');
  ------------------------

[Usage: updating-columns]
  Updating value specified existing column using column-names(or column-positions) and column-values.
  e.g.) --command=updating-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-names=b --column-values=NULL
        --command=updating-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-positions=2 --column-values=NULL
  ------------------------
  insert into xxxx (a, b, c) values ('123', 1, '0');
  ------------------------
    ↓
  ------------------------
  insert into xxxx (a, b, c) values ('123', NULL, '0');
  ------------------------

[Usage: ordering-columns]
  Ordering column specified order using column-names(or column-positions).
  e.g.) --command=ordering-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-names=c,a,b
        --command=ordering-columns --dir=src/test/resources/data --files=xxx.sql,yyy.sql --column-positions=3,1,2
  ------------------------
  insert into xxxx (a, b, c) values ('123', 1, '0');
  ------------------------
    ↓
  ------------------------
  insert into xxxx (c, a, b) values ('0', '123', 1);
  ------------------------

[Usage: formatting]
  Formatting sql to one line format and separate column list using ', '.
  e.g.) --command=formatting --dir=src/test/resources/data --files=xxx.sql,yyy.sql
  ------------------------
  insert into xxxx
     (a,b,c) values ('123',    1,    '0');
  ------------------------
    ↓
  ------------------------
  insert into xxxx (a, b, c) values ('123', 1, '0');
  ------------------------

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  2.828 s
[INFO] Finished at: 2021-11-01T02:28:57+09:00
[INFO] ------------------------------------------------------------------------
```

### Using standalone Java Application

```bash
$ ./mvnw clean verify -DskipTests
```

```
$ java -jar target/sql-bulk-commands.jar
```

