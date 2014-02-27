/**
 * $Author: nmarangoni $
 * $Date: 2011-11-11 16:22:42 +0100 (Fr, 11 Nov 2011) $
 * $Revision: 1709 $
 * $Id: install_core.sql 1709 2011-11-11 15:22:42Z nmarangoni $
 * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/install_core.sql $
 */

-- Packages
@packages\pkg_etl_framework\pkg_etl_framework-def.sql;
@packages\pkg_etl_framework\pkg_etl_framework-impl.sql;

-- Grants
@grants\enable.sql;