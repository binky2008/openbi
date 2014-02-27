CREATE OR REPLACE PACKAGE pkg_etl_stage_build AUTHID CURRENT_USER
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-07-30 16:17:55 +0200 (Mo, 30 Jul 2012) $
    * $Revision: 3082 $
    * $Id: pkg_etl_stage_build-def.sql 3082 2012-07-30 14:17:55Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_build/pkg_etl_stage_build-def.sql $
    */

   /**
    * Package spec version string.
    */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: pkg_etl_stage_build-def.sql 3082 2012-07-30 14:17:55Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_etl_stage_build/pkg_etl_stage_build-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version            VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                VARCHAR2 (1024);

   /**
    * Build all target objects
    */
   PROCEDURE prc_build_all (
      p_vc_source_code    VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name    VARCHAR2 DEFAULT 'ALL'
    , p_b_indx_st1_flag   BOOLEAN DEFAULT FALSE
    , p_b_drop_st1_flag   BOOLEAN DEFAULT TRUE
    , p_b_drop_st2_flag   BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   );

   /**
    * Build stg2 only target objects
    */
   PROCEDURE prc_build_tc_only (
      p_vc_source_code    VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name    VARCHAR2 DEFAULT 'ALL'
    , p_b_indx_st1_flag   BOOLEAN DEFAULT FALSE
    , p_b_drop_st1_flag   BOOLEAN DEFAULT TRUE
    , p_b_drop_st2_flag   BOOLEAN DEFAULT FALSE
    , p_b_raise_flag      BOOLEAN DEFAULT FALSE
   );

   /**
    * Upgrade stage2 table
    */
   PROCEDURE prc_upgrade_stage2 (
      p_vc_source_code   VARCHAR2
    , p_vc_object_name   VARCHAR2
   );
END pkg_etl_stage_build;
/

SHOW errors