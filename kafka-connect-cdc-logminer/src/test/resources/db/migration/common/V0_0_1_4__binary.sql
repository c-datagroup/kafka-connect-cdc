CREATE TABLE datatype_testing.blob_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value BLOB
);
CREATE TABLE datatype_testing.clob_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value CLOB
);
CREATE TABLE datatype_testing.nclob_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value NCLOB
);