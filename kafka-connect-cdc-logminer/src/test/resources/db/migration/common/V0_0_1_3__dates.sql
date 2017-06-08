CREATE TABLE datatype_testing.date_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value DATE
);
CREATE TABLE datatype_testing.timestamp_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value TIMESTAMP
);
CREATE TABLE datatype_testing.timestamp_with_tz_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value TIMESTAMP WITH TIME ZONE
);
CREATE TABLE datatype_testing.timestamp_with_ltz_table (
  ID    NUMBER(10) NOT NULL PRIMARY KEY,
  value TIMESTAMP WITH LOCAL TIME ZONE
);