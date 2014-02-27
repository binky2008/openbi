CREATE OR REPLACE PACKAGE pkg_etl_stage_stat AUTHID CURRENT_USER
AS
   /**
    * Package containing tools to collect statistics and size of STAGE tables
    *
    * $Author: nmarangoni $
    * $Date: 2011-10-05 13:37:55 +0200 (Mi, 05 Okt 2011) $
    * $Revision: 1566 $
    * $Id: pkg_etl_stage_stat-def.sql 1566 2011-10-05 11:37:55Z nmarangoni $
    * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_stat/pkg_etl_stage_stat-def.sql $
    */

   /**
    * Package spec version string.
    */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: pkg_etl_stage_stat-def.sql 1566 2011-10-05 11:37:55Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_stat/pkg_etl_stage_stat-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version            VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                VARCHAR2 (1024);

   /**
    * Set global load id
    */
   PROCEDURE prc_set_load_id;

   /**
    * Create a synonym for a given object
    *
    * @param p_vc_source_code       Source name
    * @param p_vc_object_name       Object name
    * @param p_n_stage_id           Stage id (1 or 2)
    * @param p_n_partition          Table partition
    * @param p_vc_stat_type_code    Statistics type
    */
   FUNCTION prc_stat_begin (
      p_vc_source_code      VARCHAR2
    , p_vc_object_name      VARCHAR2
    , p_n_stage_id          NUMBER DEFAULT NULL
    , p_n_partition         NUMBER DEFAULT NULL
    , p_vc_stat_type_code   VARCHAR2 DEFAULT NULL
   )
      RETURN NUMBER;

   PROCEDURE prc_stat_end (
      p_n_stat_id      NUMBER
    , p_n_stat_value   NUMBER DEFAULT 0
    , p_n_stat_error   NUMBER DEFAULT 0
   );

   PROCEDURE prc_stat_purge;

   PROCEDURE prc_size_store (
      p_vc_source_code   VARCHAR2
    , p_vc_object_name   VARCHAR2
    , p_vc_table_name    VARCHAR2
   );
END pkg_etl_stage_stat;
/

SHOW errors