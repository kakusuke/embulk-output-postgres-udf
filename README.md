# PostgreSQL UDF output plugin for Embulk

Dumps records to PostgreSQL via user-defined function.

## Overview

* **Plugin type**: output
* **Resume supported**: no

## Configuration

- **host**: database host name description (string, required)
- **port**: database port number (integer, default: 5432)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: database login password (string, default: "public")
- **function**: function body (string, required)
- **language**: function language (string, default: "plpgsql")

## Example

```yaml
in:
  type: file
  path_prefix: /path/to/csv/sample_
  parser:
    charset: UTF-8
    newline: CRLF
    type: csv
    delimiter: ','
    quote: '"'
    escape: ''
    skip_header_lines: 1
    columns:
    - {name: id, type: long}
    - {name: account, type: long}
    - {name: time, type: timestamp, format: '%Y-%m-%d %H:%M:%S'}
    - {name: purchase, type: timestamp, format: '%Y%m%d'}
    - {name: comment, type: string}
out:
  type: postgres_udf
  host: localhost
  user: postgres
  database: postgres
  function: |
    begin
      begin
        create table if not exists sample (
          id integer,
          account integer,
          time timestamp,
          purchase timestamp,
          comment text
        );
      exception
        when unique_violation then -- do nothing
      end;
      insert into sample values(id, account, time, purchase, comment);
    end;
```


## Build

```
$ ./gradlew gem
```
