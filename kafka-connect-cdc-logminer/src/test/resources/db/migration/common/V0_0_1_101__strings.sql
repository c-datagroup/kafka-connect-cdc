INSERT INTO datatype_testing.char_table (id, value) VALUES (1, 'this is a char value');
INSERT INTO datatype_testing.varchar_table (id, value) VALUES (1, 'this is a char value');
INSERT INTO datatype_testing.varchar2_table (id, value) VALUES (1, 'this is a char value');
INSERT INTO datatype_testing.nchar_table (id, value) VALUES (1, 'this is a char value');
INSERT INTO datatype_testing.nvarchar2_table (id, value) VALUES (1, 'this is a char value');

INSERT INTO datatype_testing.char_table (id, value) VALUES (2, 'this is another value');
INSERT INTO datatype_testing.varchar_table (id, value) VALUES (2, 'this is another value');
INSERT INTO datatype_testing.varchar2_table (id, value) VALUES (2, 'this is another value');
INSERT INTO datatype_testing.nchar_table (id, value) VALUES (2, 'this is another value');
INSERT INTO datatype_testing.nvarchar2_table (id, value) VALUES (2, 'this is another value');

UPDATE datatype_testing.char_table SET value = 'this is another value' WHERE ID = 2;
UPDATE datatype_testing.varchar_table SET value = 'this is another value' WHERE ID = 2;
UPDATE datatype_testing.varchar2_table SET value = 'this is another value' WHERE ID = 2;
UPDATE datatype_testing.nchar_table SET value = 'this is another value' WHERE ID = 2;
UPDATE datatype_testing.nvarchar2_table SET value = 'this is another value' WHERE ID = 2;

DELETE FROM datatype_testing.char_table  WHERE ID = 2;
DELETE FROM datatype_testing.varchar_table  WHERE ID = 2;
DELETE FROM datatype_testing.varchar2_table  WHERE ID = 2;
DELETE FROM datatype_testing.nchar_table  WHERE ID = 2;
DELETE FROM datatype_testing.nvarchar2_table  WHERE ID = 2;