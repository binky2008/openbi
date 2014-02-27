CREATE OR REPLACE PACKAGE pkg_etl_stage_meta AUTHID CURRENT_USER
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-06-20 15:27:31 +0200 (Mi, 20 Jun 2012) $
    * $Revision: 2876 $
    * $Id: pkg_etl_stage_meta-def.sql 2876 2012-06-20 13:27:31Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_meta/pkg_etl_stage_meta-def.sql $
    */

   /**
    * Package spec version string.
    */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: pkg_etl_stage_meta-def.sql 2876 2012-06-20 13:27:31Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_meta/pkg_etl_stage_meta-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version            VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                VARCHAR2 (1024);

   FUNCTION fct_get_column_list (
      p_vc_object_id     IN   NUMBER
    , p_vc_column_type   IN   VARCHAR2
    , p_vc_list_type     IN   VARCHAR2
    , p_vc_alias1        IN   VARCHAR2 DEFAULT NULL
    , p_vc_alias2        IN   VARCHAR2 DEFAULT NULL
   )
      RETURN VARCHAR2;

   PROCEDURE prc_stat_type_ins (
      p_vc_type_code   IN   VARCHAR2
    , p_vc_type_name   IN   VARCHAR2
    , p_vc_type_desc   IN   VARCHAR2
   );

   PROCEDURE prc_source_ins (
      p_vc_source_code      IN   VARCHAR2
    , p_vc_source_prefix    IN   VARCHAR2
    , p_vc_source_name      IN   VARCHAR2
    , p_vc_stage_owner      IN   VARCHAR2
    , p_vc_ts_stg1_data     IN   VARCHAR2
    , p_vc_ts_stg1_indx     IN   VARCHAR2
    , p_vc_ts_stg2_data     IN   VARCHAR2
    , p_vc_ts_stg2_indx     IN   VARCHAR2
    , p_vc_fb_archive       IN   VARCHAR2 DEFAULT NULL
    , p_vc_bodi_ds          IN   VARCHAR2 DEFAULT NULL
    , p_vc_source_bodi_ds   IN   VARCHAR2 DEFAULT NULL
   );

   PROCEDURE prc_source_del (
      p_vc_source_code   IN   VARCHAR2
    , p_b_cascade        IN   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_source_db_ins (
      p_vc_source_code          IN   VARCHAR2
    , p_vc_distribution_code    IN   VARCHAR2
    , p_vc_source_db_link       IN   VARCHAR2
    , p_vc_source_owner         IN   VARCHAR2
    , p_vc_source_db_jdbcname   IN   VARCHAR2 DEFAULT NULL
    , p_vc_source_bodi_ds       IN   VARCHAR2 DEFAULT NULL
   );

   PROCEDURE prc_object_ins (
      p_vc_source_code        IN   VARCHAR2
    , p_vc_object_name        IN   VARCHAR2
    , p_n_parallel_degree     IN   NUMBER DEFAULT NULL
    , p_vc_filter_clause      IN   VARCHAR2 DEFAULT NULL
    , p_vc_partition_clause   IN   VARCHAR2 DEFAULT NULL
    , p_vc_fbda_flag          IN   NUMBER DEFAULT NULL
    , p_vc_increment_buffer   IN   NUMBER DEFAULT NULL
    , p_vc_std_load_modus     IN   VARCHAR2 DEFAULT NULL
   );

   PROCEDURE prc_object_del (
      p_vc_source_code   IN   VARCHAR2
    , p_vc_object_name   IN   VARCHAR2
    , p_b_cascade        IN   BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_column_ins (
      p_vc_source_code       IN   VARCHAR2
    , p_vc_object_name       IN   VARCHAR2
    , p_vc_column_name       IN   VARCHAR2
    , p_vc_column_name_map   IN   VARCHAR2 DEFAULT NULL
    , p_vc_column_def        IN   VARCHAR2 DEFAULT NULL
    , p_n_column_pos         IN   NUMBER DEFAULT NULL
    , p_n_column_nk_pos      IN   NUMBER DEFAULT NULL
    , p_n_column_incr_flag   IN   NUMBER DEFAULT 0
    , p_n_column_hist_flag   IN   NUMBER DEFAULT 1
    , p_n_column_edwh_flag   IN   NUMBER DEFAULT 1
   );

   PROCEDURE prc_column_del (
      p_vc_source_code   IN   VARCHAR2
    , p_vc_object_name   IN   VARCHAR2
    , p_vc_column_name   IN   VARCHAR2
   );

   PROCEDURE prc_curr_hist_ins (
      p_vc_source_code        IN   VARCHAR2
    , p_vc_curr_object_name   IN   VARCHAR2
    , p_vc_hist_object_name   IN   VARCHAR2
   );

   PROCEDURE prc_column_import (
      p_vc_source_code         IN   VARCHAR2
    , p_vc_object_name         IN   VARCHAR2 DEFAULT 'ALL'
    , p_b_check_dependencies   IN   BOOLEAN DEFAULT TRUE
   );

   PROCEDURE prc_column_import_from_stg1 (
      p_vc_source_code         IN   VARCHAR2
    , p_vc_object_name         IN   VARCHAR2 DEFAULT 'ALL'
    , p_b_check_dependencies   IN   BOOLEAN DEFAULT TRUE
   );

   PROCEDURE prc_check_column_changes (
      p_vc_source_code         IN   VARCHAR2
    , p_vc_object_name         IN   VARCHAR2 DEFAULT 'ALL'
    , p_b_check_dependencies   IN   BOOLEAN DEFAULT TRUE
   );

   PROCEDURE prc_set_object_properties;
END pkg_etl_stage_meta;
/

SHOW errors