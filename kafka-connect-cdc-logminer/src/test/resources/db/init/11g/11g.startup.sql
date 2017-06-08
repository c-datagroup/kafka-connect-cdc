SET ECHO ON;

SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE FORCE LOGGING;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA ( ALL ) COLUMNS;
ALTER DATABASE OPEN;

/*
Configure Oracle XStream
 */


CREATE TABLESPACE xstream_tbs DATAFILE 'xstream_tbs.dbf'
SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;

CREATE USER xstrmadmin IDENTIFIED BY lfnjgksdfbdk
DEFAULT TABLESPACE xstream_tbs
QUOTA UNLIMITED ON xstream_tbs;

GRANT CREATE SESSION TO xstrmadmin;
GRANT SYSDBA TO xstrmadmin;
GRANT DBA TO xstrmadmin;
GRANT CONNECT TO xstrmadmin;
GRANT RESOURCE TO xstrmadmin;
GRANT CREATE TABLESPACE TO xstrmadmin;
GRANT UNLIMITED TABLESPACE TO xstrmadmin;
GRANT SELECT_CATALOG_ROLE TO xstrmadmin;
GRANT EXECUTE_CATALOG_ROLE TO xstrmadmin;
GRANT CREATE SEQUENCE TO xstrmadmin;
GRANT CREATE SESSION TO xstrmadmin;
GRANT CREATE ANY VIEW TO xstrmadmin;
GRANT CREATE ANY TABLE TO xstrmadmin;
GRANT SELECT ANY TABLE TO xstrmadmin;
GRANT COMMENT ANY TABLE TO xstrmadmin;
GRANT LOCK ANY TABLE TO xstrmadmin;
GRANT SELECT ANY DICTIONARY TO xstrmadmin;
GRANT EXECUTE ON SYS.DBMS_CDC_PUBLISH TO xstrmadmin;
GRANT CREATE ANY TRIGGER TO xstrmadmin;
GRANT ALTER ANY TRIGGER TO xstrmadmin;
GRANT DROP ANY TRIGGER TO xstrmadmin;

-- BEGIN
--   DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
--       grantee                 => 'xstrmadmin',
--       privilege_type          => 'CAPTURE',
--       grant_select_privileges => TRUE);
-- END;
-- /
--
-- BEGIN
--   DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
--       grantee                 => 'xstrmadmin',
--       privilege_type          => 'APPLY',
--       grant_select_privileges => TRUE);
-- END;
-- /

exec dbms_streams_auth.grant_admin_privilege('xstrmadmin', true);

DECLARE
  tables  DBMS_UTILITY.UNCL_ARRAY;
  schemas DBMS_UTILITY.UNCL_ARRAY;
BEGIN
  schemas(1) := 'CDC_TESTING';
  tables(1) := NULL;
  DBMS_XSTREAM_ADM.CREATE_OUTBOUND(
      server_name     =>  'xout',
      table_names     =>  tables,
      schema_names    =>  schemas
  );
END;
/

EXIT;