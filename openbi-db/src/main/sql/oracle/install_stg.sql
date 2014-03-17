/**
 * $Author: nmarangoni $
 * $Date: 2012-02-02 16:38:28 +0100 (Do, 02 Feb 2012) $
 * $Revision: 2289 $
 * $Id: install_stage.sql 2289 2012-02-02 15:38:28Z nmarangoni $
 * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/install_stage.sql $
 */

@tables\stg_column_t.sql;
@tables\stg_column_check_t.sql;
@tables\stg_column_tmp.sql;
@tables\stg_ddl_t.sql;
@tables\stg_object_t.sql;
@tables\stg_queue_t.sql;
@tables\stg_queue_object_t.sql;
@tables\stg_source_db_t.sql;
@tables\stg_source_t.sql;
@tables\stg_stat_type_t.sql;
@tables\stg_stat_t.sql;
@tables\stg_size_t.sql;

-- Views
@views\stg_column_v.sql;
@views\stg_column_check_v.sql;
@views\stg_object_v.sql;
@views\stg_queue_object_v.sql;
@views\stg_queue_v.sql;
@views\stg_source_db_v.sql;
@views\stg_source_v.sql;
@views\stg_stat_last_v.sql;
@views\stg_stat_v.sql;
@views\stg_size_v.sql;

-- Packages
@packages\stg_param\stg_param-def.sql;
@packages\stg_stat\stg_stat-def.sql;
@packages\stg_stat\stg_stat-impl.sql;
@packages\stg_meta\stg_meta-def.sql;
@packages\stg_meta\stg_meta-impl.sql;
@packages\stg_ddl\stg_ddl-def.sql;
@packages\stg_ddl\stg_ddl-impl.sql;
@packages\stg_build\stg_build-def.sql;
@packages\stg_build\stg_build-impl.sql;
@packages\stg_ctl\stg_ctl-def.sql;
@packages\stg_ctl\stg_ctl-impl.sql;

-- Grants
--@grants\enable.sql;