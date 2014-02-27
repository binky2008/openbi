/**
 * $Author: nmarangoni $
 * $Date: 2012-02-02 16:38:28 +0100 (Do, 02 Feb 2012) $
 * $Revision: 2289 $
 * $Id: install_stage.sql 2289 2012-02-02 15:38:28Z nmarangoni $
 * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/install_stage.sql $
 */

@tables\etl_stage_column_t.sql;
@tables\etl_stage_column_check_t.sql;
@tables\etl_stage_column_tmp.sql;
@tables\etl_stage_ddl_t.sql;
@tables\etl_stage_object_t.sql;
@tables\etl_stage_queue_t.sql;
@tables\etl_stage_queue_object_t.sql;
@tables\etl_stage_source_db_t.sql;
@tables\etl_stage_source_t.sql;
@tables\etl_stage_stat_type_t.sql;
@tables\etl_stage_stat_t.sql;
@tables\etl_stage_size_t.sql;
@tables\etl_stage_curr_hist_t.sql;

-- Views
@views\etl_stage_column_v.sql;
@views\etl_stage_column_check_v.sql;
@views\etl_stage_object_v.sql;
@views\etl_stage_queue_object_v.sql;
@views\etl_stage_queue_v.sql;
@views\etl_stage_source_db_v.sql;
@views\etl_stage_source_v.sql;
@views\etl_stage_stat_last_v.sql;
@views\etl_stage_stat_v.sql;
@views\etl_stage_size_v.sql;
@views\etl_stage_curr_hist_v.sql;

-- Packages
@packages\pkg_etl_stage_param\pkg_etl_stage_param-def.sql;
@packages\pkg_etl_stage_stat\pkg_etl_stage_stat-def.sql;
@packages\pkg_etl_stage_stat\pkg_etl_stage_stat-impl.sql;
@packages\pkg_etl_stage_meta\pkg_etl_stage_meta-def.sql;
@packages\pkg_etl_stage_meta\pkg_etl_stage_meta-impl.sql;
@packages\pkg_etl_stage_ddl\pkg_etl_stage_ddl-def.sql;
@packages\pkg_etl_stage_ddl\pkg_etl_stage_ddl-impl.sql;
@packages\pkg_etl_stage_build\pkg_etl_stage_build-def.sql;
@packages\pkg_etl_stage_build\pkg_etl_stage_build-impl.sql;
@packages\pkg_etl_stage_ctl\pkg_etl_stage_ctl-def.sql;
@packages\pkg_etl_stage_ctl\pkg_etl_stage_ctl-impl.sql;

-- Grants
@grants\enable.sql;