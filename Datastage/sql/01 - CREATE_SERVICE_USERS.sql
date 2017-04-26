--Service DWH users:

--Main schema DWH  ------------------------------------------- 
CREATE USER DWH  IDENTIFIED BY dwhadm -- it's password
PROFILE DEFAULT
ACCOUNT UNLOCK;
 
GRANT CONNECT TO DWH; 
ALTER USER DWH DEFAULT ROLE ALL;

  -- 1 System Privilege for DWH 
GRANT CREATE SESSION TO DWH;
ALTER USER DWH QUOTA UNLIMITED ON USERS;
-------------------------------------------------------------

--Service user for all DWH operations -------------------------- 
CREATE USER DWHADM  IDENTIFIED BY dwhadm -- it's password
PROFILE DEFAULT
ACCOUNT UNLOCK;
 
GRANT CONNECT TO DWHADM; 
ALTER USER DWHADM DEFAULT ROLE ALL;

  -- 1 System Privilege for DWHADM 
GRANT CREATE SESSION TO DWHADM;
ALTER USER DWHADM QUOTA UNLIMITED ON USERS;
-------------------------------------------------------------

--Service user for metadata operations-----------------------
CREATE USER CONTROL_DWP  IDENTIFIED BY dwhadm -- it's password
PROFILE DEFAULT
ACCOUNT UNLOCK;
 
GRANT CONNECT TO CONTROL_DWP; 
ALTER USER CONTROL_DWP DEFAULT ROLE ALL;

  -- 1 System Privilege for CONTROL_DWP 
GRANT CREATE SESSION TO CONTROL_DWP;
ALTER USER CONTROL_DWP QUOTA UNLIMITED ON USERS;
-------------------------------------------------------------