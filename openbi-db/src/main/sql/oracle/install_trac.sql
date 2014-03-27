/**
 * $Author: nmarangoni $
 * $Date: 2012-02-02 16:38:28 +0100 (Do, 02 Feb 2012) $
 * $Revision: 2289 $
 * $Id: install_stage.sql 2289 2012-02-02 15:38:28Z nmarangoni $
 * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/install_stage.sql $
 */

@tables\trac_t.sql;

-- Views
@views\trac_v.sql;

-- Packages
@packages\trac_param\trac_param-def.sql;
@packages\trac\trac-def.sql;
@packages\trac\trac-impl.sql;
