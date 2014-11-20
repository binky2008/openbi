/**
 * $Author: nmarangoni $
 * $Date: 2012-06-29 13:52:44 +0200 (Fr, 29 Jun 2012) $
 * $Revision: 2914 $
 * $Id: install_utl.sql 2914 2012-06-29 11:52:44Z nmarangoni $
 * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/install_utl.sql $
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

