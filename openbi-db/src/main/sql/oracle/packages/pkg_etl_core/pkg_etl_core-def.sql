CREATE OR REPLACE PACKAGE pkg_etl_core AUTHID CURRENT_USER
AS
   /**
   * $Author: nmarangoni $
   * $Date: 2011-10-20 13:10:16 +0200 (Do, 20 Okt 2011) $
   * $Revision: 1631 $
   * $Id: pkg_etl_core-def.sql 1631 2011-10-20 11:10:16Z nmarangoni $
   * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_core/pkg_etl_core-def.sql $
   */
   e_lock_detected           EXCEPTION;
   PRAGMA EXCEPTION_INIT (e_lock_detected, -54);
   /**
    * Package spec version string.
    */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: pkg_etl_core-def.sql 1631 2011-10-20 11:10:16Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_core/pkg_etl_core-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version            VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                VARCHAR2 (1024);

   /**
    * Write statistics in the UTL_LOAD_STATISTICS_T
    */
   PROCEDURE write_statistics (
      p_tgt_tab              IN   VARCHAR
    , p_di_system            IN   VARCHAR
    , p_di_gui               IN   NUMBER
    , p_rows_ins             IN   NUMBER
    , p_rows_upd             IN   NUMBER
    , p_rows_upd_versioned   IN   NUMBER
    , p_rows_del             IN   NUMBER
    , p_rows_del_versioned   IN   NUMBER
   );

   /**
    * Select the value of a given sequence otherwise return the passed value
    */
   FUNCTION get_sequence (
      p_sequence_name   IN   VARCHAR2
    , p_sequence_val    IN   NUMBER
   )
      RETURN NUMBER;

   /**
    * FUNCTION fill_master_tab
    * Blah
    * Blah.
    */
   FUNCTION fill_master_tab (
      p_job_name         IN       VARCHAR2
    , p_workflow_name    IN       VARCHAR2
    , p_di_gui           IN       NUMBER
    , p_src_table_name   IN       VARCHAR2
    , p_tgt_table_name   IN       VARCHAR2
    , p_log_message      OUT      VARCHAR2
    , p_do_not_execute   IN       VARCHAR2 DEFAULT 'N'
   )
      RETURN NUMBER;

   /**
    * FUNCTION fill_master_tab_multi_nk
    * Blah
    * Blah.
    */
   FUNCTION fill_master_tab_multi_nk (
      p_job_name             IN       VARCHAR2
    , p_workflow_name        IN       VARCHAR2
    , p_di_gui               IN       NUMBER
    , p_src_table_name       IN       VARCHAR2
    , p_tgt_table_name       IN       VARCHAR2
    , p_src_nk_column_list   IN       VARCHAR2
    , p_tgt_nk_column_list   IN       VARCHAR2
    , p_log_message          OUT      VARCHAR2
    , p_do_not_execute       IN       VARCHAR2 DEFAULT 'N'
   )
      RETURN NUMBER;

   /**
    * FUNCTION fill_hist_tab
    * Blah
    * Blah.
    */
   FUNCTION fill_hist_tab (
      p_job_name          IN       VARCHAR2
    , p_workflow_name     IN       VARCHAR2
    , p_di_gui            IN       NUMBER
    , p_hist_table_name   IN       VARCHAR2
    , p_log_message       OUT      VARCHAR2
    , p_cutoff_day                 DATE DEFAULT NULL
    , p_do_not_execute    IN       VARCHAR2 DEFAULT 'N'
   )
      RETURN NUMBER;

   /**
    * FUNCTION fill_r_tab
    * Framework for the R Tables Dataload
    *
    * Pecondition:
    *  a table temporary table in the EDWH_CL schema is required
    * Description:
    *  this function has more Steps:
    *  - all new records in the B Table, must be saved in the R Table
    *  - identification R Table's Periods to be delete  (obsolete Periods)
    *  - identify all changes between Target and Source
    *  - R table update (period change) - versioning and inplace updates
    *  - R table inserts - new periods
   */
   FUNCTION fill_r_tab (
      p_job_name              IN       VARCHAR2
    , p_workflow_name         IN       VARCHAR2
    , p_di_gui                IN       NUMBER
    , p_src_desc              IN       VARCHAR2                                                                                                             -- Table abbrevation e.g. CONTRACT,CUSTOMER
    , p_src_name              IN       VARCHAR2                                                                                                                         -- synonym e.g. EDWH_CORE.OCONT
    , p_sk_column_name_r      IN       VARCHAR2
    , p_sk_column_name_src    IN       VARCHAR2
    , p_nk_column_name_b1     IN       VARCHAR2
    , p_nk_column_name_src1   IN       VARCHAR2
    , p_nk_column_name_b2     IN       VARCHAR2                                                                                                                                   -- optional second NK
    , p_nk_column_name_src2   IN       VARCHAR2                                                                                                                                   -- optional second NK
    , p_id_column_name_b      IN       VARCHAR2
    , p_id_column_name_src    IN       VARCHAR2
    , p_log_message           OUT      VARCHAR2
    , p_nk_src_is_varchar     IN       VARCHAR2 DEFAULT 'N'
    , p_do_not_execute        IN       VARCHAR2 DEFAULT 'N'
   )
      RETURN NUMBER;
END pkg_etl_core;
/

SHOW errors

/*BEGIN
   ddl.prc_create_synonym ('pkg_etl_core'
                                 , 'pkg_etl_core'
                                 , TRUE
                                  );
END;
/*/

SHOW errors