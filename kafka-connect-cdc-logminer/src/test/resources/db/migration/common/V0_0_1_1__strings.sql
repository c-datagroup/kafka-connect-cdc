CREATE TABLE datatype_testing.char_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  VALUE CHAR(128)
);
CREATE TABLE datatype_testing.varchar_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value VARCHAR(128)
);
CREATE TABLE datatype_testing.varchar2_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value VARCHAR2(128)
);
CREATE TABLE datatype_testing.nchar_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value NCHAR(128)
);
CREATE TABLE datatype_testing.nvarchar2_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value NVARCHAR2(128)
);