SET ECHO ON;

ALTER SESSION SET CONTAINER=CDB$ROOT;
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE FORCE LOGGING;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER DATABASE OPEN;

ALTER SESSION SET CONTAINER = ORCLPDB1;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

CREATE TABLESPACE datatype_tbs DATAFILE 'datatype_tbs.dat' SIZE 10M AUTOEXTEND ON;
CREATE TEMPORARY TABLESPACE datatype_tbs_temp TEMPFILE 'datatype_tbs_temp.dat' SIZE 10M AUTOEXTEND ON;
CREATE USER datatype_testing IDENTIFIED BY 1234 DEFAULT TABLESPACE datatype_tbs TEMPORARY TABLESPACE datatype_tbs_temp;
ALTER USER datatype_testing quota unlimited on datatype_tbs;

ALTER SESSION SET CONTAINER=CDB$ROOT;

ALTER PLUGGABLE DATABASE ALL OPEN;
CREATE USER c##xstrmadmin IDENTIFIED BY lfnjgksdfbdk;
ALTER USER c##xstrmadmin QUOTA UNLIMITED ON USERS;
GRANT SYSDBA TO c##xstrmadmin CONTAINER=ALL;
GRANT DBA TO c##xstrmadmin CONTAINER = ALL;
GRANT CONNECT TO c##xstrmadmin CONTAINER = ALL;
GRANT RESOURCE TO c##xstrmadmin CONTAINER = ALL;
GRANT CREATE TABLESPACE TO c##xstrmadmin CONTAINER = ALL;
GRANT UNLIMITED TABLESPACE TO c##xstrmadmin CONTAINER = ALL;
GRANT SELECT_CATALOG_ROLE TO c##xstrmadmin CONTAINER = ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##xstrmadmin CONTAINER = ALL;
GRANT CREATE SEQUENCE TO c##xstrmadmin CONTAINER = ALL;
GRANT CREATE SESSION, SET CONTAINER TO c##xstrmadmin CONTAINER=ALL;
GRANT CREATE ANY VIEW TO c##xstrmadmin CONTAINER = ALL;
GRANT CREATE ANY TABLE TO c##xstrmadmin CONTAINER = ALL;
GRANT SELECT ANY TABLE TO c##xstrmadmin CONTAINER = ALL;
GRANT COMMENT ANY TABLE TO c##xstrmadmin CONTAINER = ALL;
GRANT LOCK ANY TABLE TO c##xstrmadmin CONTAINER = ALL;
GRANT SELECT ANY DICTIONARY TO c##xstrmadmin CONTAINER = ALL;
GRANT EXECUTE ON SYS.DBMS_CDC_PUBLISH to c##xstrmadmin CONTAINER = ALL;
GRANT CREATE ANY TRIGGER TO c##xstrmadmin CONTAINER = ALL;
GRANT ALTER ANY TRIGGER TO c##xstrmadmin CONTAINER = ALL;
GRANT DROP ANY TRIGGER TO c##xstrmadmin CONTAINER = ALL;
ALTER USER c##xstrmadmin QUOTA UNLIMITED ON USERS;


BEGIN
  DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
      grantee                 => 'c##xstrmadmin',
      privilege_type          => 'CAPTURE',
      do_grants               => TRUE,
      grant_select_privileges => TRUE,
      container               => 'ORCLPDB1'
  );
END;
/


BEGIN
  DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
      grantee                 => 'c##xstrmadmin',
      privilege_type          => 'APPLY',
      do_grants               => TRUE,
      grant_select_privileges => TRUE,
      container               => 'ORCLPDB1'
  );
END;
/

DECLARE
  tables  DBMS_UTILITY.UNCL_ARRAY;
  schemas DBMS_UTILITY.UNCL_ARRAY;
BEGIN
  schemas(1) := 'DATATYPE_TESTING';
  tables(1) := null;
  DBMS_XSTREAM_ADM.CREATE_OUTBOUND(
      server_name     =>  'xout',
      source_database =>  'ORCLPDB1',
      schema_names    =>  schemas,
      table_names     =>  tables,
      connect_user    =>  'c##xstrmadmin',
      capture_user    =>  'c##xstrmadmin'
  );
END;
/

PROMPT XSTREAM IS CONFIGURED

EXIT;