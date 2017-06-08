CREATE TABLE datatype_testing.number_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value NUMBER(18, 2)
);
CREATE TABLE datatype_testing.binary_float_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value BINARY_FLOAT
);
CREATE TABLE datatype_testing.binary_double_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value BINARY_DOUBLE
);