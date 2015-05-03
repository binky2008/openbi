/**
 * $Author: admin $
 * $Date: 2015-05-03 17:35:13 +0200 (So, 03 Mai 2015) $
 * $Revision: 5 $
 * $Id: install_tool.sql 5 2015-05-03 15:35:13Z admin $
 * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/install_tool.sql $
 */

purge recyclebin;
-- Packages
@packages\type\type-def.sql;
@packages\type\type-impl.sql;
@packages\dict\dict-def.sql;
@packages\dict\dict-impl.sql;
@packages\stmt\stmt-def.sql;
@packages\stmt\stmt-impl.sql;
@packages\ddls\ddls-def.sql;
@packages\ddls\ddls-impl.sql;
@packages\auth\auth-def.sql;
@packages\auth\auth-impl.sql;
@packages\enbl\enbl-def.sql;
@packages\enbl\enbl-impl.sql;

