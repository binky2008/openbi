/**
 * $Author: admin $
 * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
 * $Revision: 15 $
 * $Id: install_stag.sql 15 2015-05-03 16:17:11Z admin $
 * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/install_stag.sql $
 */

@tables\stag_column_t.sql;
@tables\stag_column_check_t.sql;
@tables\stag_ddl_t.sql
@tables\stag_object_t.sql;
@tables\stag_object_uk.sql;
@tables\stag_queue_t.sql;
@tables\stag_queue_uk.sql;
@tables\stag_queue_object_t.sql;
@tables\stag_queue_object_uk.sql;
@tables\stag_source_db_t.sql;
@tables\stag_source_t.sql;
@tables\stag_stat_type_t.sql;
@tables\stag_stat_t.sql;
@tables\stag_size_t.sql;
@tables\stag_size_uk.sql;

-- Views
@views\stag_column_v.sql;
@views\stag_column_check_v.sql;
@views\stag_object_v.sql;
@views\stag_queue_object_v.sql;
@views\stag_queue_v.sql;
@views\stag_source_db_v.sql;
@views\stag_source_v.sql;
@views\stag_stat_v.sql;
@views\stag_size_v.sql;

-- Packages
@packages\stag_param\stag_param-def.sql;
@packages\stag_stat\stag_stat-def.sql;
@packages\stag_stat\stag_stat-impl.sql;
@packages\stag_meta\stag_meta-def.sql;
@packages\stag_meta\stag_meta-impl.sql;
@packages\stag_ddl\stag_ddl-def.sql;
@packages\stag_ddl\stag_ddl-impl.sql;
@packages\stag_build\stag_build-def.sql;
@packages\stag_build\stag_build-impl.sql;
@packages\stag_ctl\stag_ctl-def.sql;
@packages\stag_ctl\stag_ctl-impl.sql;
-- Data
@tables\stag_stat_type_data.sql;