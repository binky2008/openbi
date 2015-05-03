CREATE OR REPLACE PACKAGE p#frm#stag_build
   AUTHID CURRENT_USER
AS
   /**
   *
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: stag_build-def.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_build/stag_build-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: stag_build-def.sql 15 2015-05-03 16:17:11Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stag_build/stag_build-def.sql $';
   /**
   * Package body version string.
   */
   c_body_version            VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                VARCHAR2 (1024);

   /**
    * Build stage target objects
    */
   PROCEDURE prc_build_all (
      p_vc_source_code     VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name     VARCHAR2 DEFAULT 'ALL'
    , p_b_index_flag    BOOLEAN DEFAULT FALSE
    , p_b_drop_stage_flag    BOOLEAN DEFAULT TRUE
    , p_b_drop_hist_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag       BOOLEAN DEFAULT FALSE
   );

   /**
    * Build hist target objects
    */
   PROCEDURE prc_build_hist (
      p_vc_source_code     VARCHAR2 DEFAULT 'ALL'
    , p_vc_object_name     VARCHAR2 DEFAULT 'ALL'
    , p_b_drop_flag    BOOLEAN DEFAULT FALSE
    , p_b_raise_flag       BOOLEAN DEFAULT FALSE
   );

   /**
    * Upgrade hist table
    */
   PROCEDURE prc_upgrade_hist (
      p_vc_source_code    VARCHAR2
    , p_vc_object_name    VARCHAR2
   );
END p#frm#stag_build;