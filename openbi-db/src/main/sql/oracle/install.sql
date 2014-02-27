/**
 * $Author: nmarangoni $
 * $Date: 2011-05-09 18:29:21 +0200 (Mo, 09 Mai 2011) $
 * $Revision: 289 $
 * $Id: pkg_etl_stage_ctl-def.sql 289 2011-05-09 16:29:21Z nmarangoni $
 * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_ctl/pkg_etl_stage_ctl-def.sql $
 */

-- Packages
@packages\pkg_param\pkg_param-def.sql;
@packages\pkg_aux_version\pkg_aux_version-def.sql;
@packages\pkg_aux_version\pkg_aux_version-impl.sql;
@packages\pkg_aux_type\pkg_aux_type-def.sql;
@packages\pkg_aux_type\pkg_aux_type-impl.sql;
@packages\pkg_aux_ddl\pkg_aux_ddl-def.sql;
@packages\pkg_aux_ddl\pkg_aux_ddl-impl.sql;

-- Tables
@tables\sys_entity_t.sql;
@tables\sys_environment_t.sql;
@tables\sys_layer_t.sql;
@tables\sys_taxonomy_t.sql;
@tables\sys_user_t.sql;
@tables\sys_user_taxonomy_t.sql;
@tables\aux_job_status_t.sql;
@tables\aux_load_statistics_t.sql;
@tables\aux_log_t.sql;
@tables\aux_parameter_t.sql;
@tables\aux_doc_t.sql;
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
@tables\qc_case_t.sql;
@tables\qc_step_t.sql;
@tables\qc_keyfigure_t.sql;
@tables\qc_threshold_t.sql;
@tables\qc_exec_t.sql;
@tables\qc_case_taxonomy_t.sql;

-- Views
@views\sys_taxonomy_v.sql;
@views\sys_user_taxonomy_v.sql;
@views\aux_log_v.sql;
@views\etl_stage_column_v.sql;
@views\etl_stage_column_check_v.sql;
@views\etl_stage_object_v.sql;
@views\etl_stage_source_db_v.sql;
@views\etl_stage_source_v.sql;
@views\etl_stage_stat_last_v.sql;
@views\etl_stage_stat_v.sql;
@views\etl_stage_size_v.sql;
@views\etl_stage_queue_v.sql;
@views\etl_stage_queue_object_v.sql;
@views\etl_stage_curr_hist_v.sql;
@views\qc_case_v.sql;
@views\qc_case_taxonomy_v.sql;
@views\qc_step_v.sql;
@views\qc_keyfigure_v.sql;
@views\qc_threshold_v.sql;
@views\qc_meta_v.sql;
@views\qc_exec_v.sql;
@views\qc_exec_verify_v.sql;
@views\qc_core_propagation_v.sql;
@views\qc_stage_operation_v.sql;
@views\qc_stage_reference_v.sql;

-- Packages
@packages\pkg_sys\pkg_sys-def.sql;
@packages\pkg_sys\pkg_sys-impl.sql;
@packages\pkg_aux_log\pkg_aux_log-def.sql;
@packages\pkg_aux_log\pkg_aux_log-impl.sql;
@packages\pkg_lkp_d_day\pkg_lkp_d_day-def.sql;
@packages\pkg_lkp_d_day\pkg_lkp_d_day-impl.sql;
@packages\pkg_aux_hash\pkg_aux_hash-def.sql;
@packages\pkg_aux_hash\pkg_aux_hash-impl.sql;
@packages\pkg_aux_parameter\pkg_aux_parameter-def.sql;
@packages\pkg_aux_parameter\pkg_aux_parameter-impl.sql;
@packages\pkg_aux_job\pkg_aux_job-def.sql;
@packages\pkg_aux_job\pkg_aux_job-impl.sql;
@packages\pkg_aux_doc\pkg_aux_doc-def.sql;
@packages\pkg_aux_doc\pkg_aux_doc-impl.sql;
@packages\pkg_aux_doc_template\pkg_aux_doc_template-def.sql;
@packages\pkg_etl_framework\pkg_etl_framework-def.sql;
@packages\pkg_etl_framework\pkg_etl_framework-impl.sql;
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
@packages\pkg_qc\pkg_qc-def.sql;
@packages\pkg_qc\pkg_qc-impl.sql;
@packages\pkg_qc_core\pkg_qc_core-def.sql;
@packages\pkg_qc_core\pkg_qc_core-impl.sql;
@packages\pkg_qc_stage\pkg_qc_stage-def.sql;
@packages\pkg_qc_stage\pkg_qc_stage-impl.sql;
@packages\pkg_enable\pkg_enable-def.sql;
@packages\pkg_enable\pkg_enable-impl.sql;

-- Grants
@grants\enable.sql;

-- Data
@data\sys_entity.sql;
@data\sys_environment.sql;
@data\sys_layer.sql;
@data\sys_taxonomy.sql;
@data\etl_stage_stat_type.sql;