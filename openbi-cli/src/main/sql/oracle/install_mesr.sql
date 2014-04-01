/**
 * $Author: nmarangoni $
 * $Date: 2012-02-02 16:38:28 +0100 (Do, 02 Feb 2012) $
 * $Revision: 2289 $
 * $Id: install_stage.sql 2289 2012-02-02 15:38:28Z nmarangoni $
 * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/install_stage.sql $
 */

-- Tables
@tables\docu_t.sql;
@tables\mesr_query_t.sql;
@tables\mesr_taxn_t.sql;
@tables\mesr_keyfigure_t.sql;
@tables\mesr_threshold_t.sql;
@tables\mesr_exec_t.sql;

-- Views
@views\mesr_query_v.sql;
@views\mesr_taxn_v.sql;
@views\mesr_keyfigure_v.sql;
@views\mesr_threshold_v.sql;
@views\mesr_exec_v.sql;
@views\mesr_exec_verify_v.sql;

-- Packages
@packages\docu_param\docu_param-def.sql;
@packages\docu\docu-def.sql;
@packages\docu\docu-impl.sql;
@packages\mesr\mesr-def.sql;
@packages\mesr\mesr-impl.sql;

-- Grants
--@grants\enable.sql;