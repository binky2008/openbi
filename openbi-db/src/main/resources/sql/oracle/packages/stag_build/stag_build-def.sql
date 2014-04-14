CREATE OR REPLACE PACKAGE stag_build
   AUTHID CURRENT_USER
AS
   /**
   *
   * $Author: $
   * $Date: $
   * $Revision: $
   * $Id: $
   * $HeadURL: $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: $';
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
      p_vc_source_code     VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name     VARCHAR2 DEFAULT 'ALL'
    , p_b_indx_st1_flag    BOOLEAN DEFAULT FALSE
    , p_b_drop_st1_flag    BOOLEAN DEFAULT TRUE
    , p_b_drop_st2_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag       BOOLEAN DEFAULT FALSE
   );

   /**
    * Build stg2 only target objects
    */
   PROCEDURE prc_build_hist (
      p_vc_source_code     VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name     VARCHAR2 DEFAULT 'ALL'
    , p_b_indx_st1_flag    BOOLEAN DEFAULT FALSE
    , p_b_drop_st1_flag    BOOLEAN DEFAULT TRUE
    , p_b_drop_st2_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag       BOOLEAN DEFAULT FALSE
   );

   /**
    * Upgrade stage2 table
    */
   PROCEDURE prc_upgrade_hist (
      p_vc_source_code    VARCHAR2
    , p_vc_object_name    VARCHAR2
   );
END stag_build;