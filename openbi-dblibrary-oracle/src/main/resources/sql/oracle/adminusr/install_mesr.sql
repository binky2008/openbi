/**
 * $Author: admin $
 * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
 * $Revision: 15 $
 * $Id: install_mesr.sql 15 2015-05-03 16:17:11Z admin $
 * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/install_mesr.sql $
 */

-- Tables
@tables\docu_t.sql;
@tables\mesr_query_t.sql;
@tables\mesr_query_uk.sql;
@tables\mesr_taxn_t.sql;
@tables\mesr_taxn_uk.sql;
@tables\mesr_keyfigure_t.sql;
@tables\mesr_keyfigure_uk.sql;
@tables\mesr_threshold_t.sql;
@tables\mesr_threshold_uk.sql;
@tables\mesr_exec_t.sql;

-- Views
@views\mesr_query_v.sql;
@views\mesr_taxn_v.sql;
@views\mesr_keyfigure_v.sql;
@views\mesr_threshold_v.sql;
@views\mesr_meta_v.sql;
@views\mesr_exec_v.sql;
@views\mesr_exec_verify_v.sql;

-- Packages
@packages\docu_param\docu_param-def.sql;
@packages\docu\docu-def.sql;
@packages\docu\docu-impl.sql;
@packages\mesr\mesr-def.sql;
@packages\mesr\mesr-impl.sql;