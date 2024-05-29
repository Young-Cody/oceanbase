#package_name:dbms_upgrade
#author: hr351303


CREATE OR REPLACE PACKAGE BODY __DBMS_UPGRADE
  PROCEDURE UPGRADE(package_name VARCHAR(1024), load_from_file BOOLEAN);
    PRAGMA INTERFACE(c, UPGRADE_SINGLE);
  PROCEDURE UPGRADE_ALL(load_from_file BOOLEAN);
    PRAGMA INTERFACE(c, UPGRADE_ALL);
END;
//
