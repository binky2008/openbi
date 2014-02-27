/**
 * $Author: nmarangoni $
 * $Date: 2012-06-29 13:52:44 +0200 (Fr, 29 Jun 2012) $
 * $Revision: 2914 $
 * $Id: install_utl.sql 2914 2012-06-29 11:52:44Z nmarangoni $
 * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/install_utl.sql $
 */

purge recyclebin;
-- Packages
@packages\aux_version\aux_version-def.sql;
@packages\aux_version\aux_version-impl.sql;
@packages\aux_type\aux_type-def.sql;
@packages\aux_type\aux_type-impl.sql;
@packages\aux_ddl\aux_ddl-def.sql;
@packages\aux_ddl\aux_ddl-impl.sql;
@packages\aux_param\aux_param-def.sql;

-- Tables
@tables\txn_entity_t.sql;
@tables\txn_environment_t.sql;
@tables\txn_layer_t.sql;
@tables\txn_taxonomy_t.sql;
@tables\txn_user_t.sql;
@tables\aux_log_t.sql;
@tables\aux_log_text_t.sql;
@tables\aux_doc_t.sql;
@tables\aux_user_t.sql;

-- Views
@views\txn_taxonomy_v.sql;
@views\txn_user_v.sql;
@views\aux_log_v.sql;

-- Packages
@packages\aux_log\aux_log-def.sql;
@packages\aux_log\aux_log-impl.sql;
@packages\aux_doc_template\aux_doc_template-def.sql;
@packages\aux_doc\aux_doc-def.sql;
@packages\aux_doc\aux_doc-impl.sql;
@packages\txn_taxonomy\txn_taxonomy-def.sql;
@packages\txn_taxonomy\txn_taxonomy-impl.sql;
/*@packages\pkg_enable\pkg_enable-def.sql;
@packages\pkg_enable\pkg_enable-impl.sql;*/

-- Data
/*@data\sys_entity.sql;
@data\sys_environment.sql;
@data\sys_layer.sql;
@data\sys_taxonomy.sql;*/

-- Synonyms
@synonyms\synonyms.sql;

-- Grants
--@grants\enable.sql;
