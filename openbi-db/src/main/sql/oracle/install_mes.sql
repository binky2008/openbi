/**
 * $Author: nmarangoni $
 * $Date: 2012-02-02 16:38:28 +0100 (Do, 02 Feb 2012) $
 * $Revision: 2289 $
 * $Id: install_stage.sql 2289 2012-02-02 15:38:28Z nmarangoni $
 * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/install_stage.sql $
 */

-- Tables
@tables\doc_t.sql;
@tables\mes_query_t.sql;
@tables\mes_txn_t.sql;
@tables\mes_keyfigure_t.sql;
@tables\mes_threshold_t.sql;
@tables\mes_exec_t.sql;

-- Views
@views\mes_query_v.sql;
@views\mes_txn_v.sql;
@views\mes_keyfigure_v.sql;
@views\mes_threshold_v.sql;
@views\mes_exec_v.sql;
@views\mes_exec_verify_v.sql;

-- Packages
@packages\doc_template\doc_template-def.sql;
@packages\doc\doc-def.sql;
@packages\doc\doc-impl.sql;
@packages\mes\mes-def.sql;
@packages\mes\mes-impl.sql;

-- Grants
--@grants\enable.sql;