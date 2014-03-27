CREATE OR REPLACE PACKAGE auth
AS
   /**
   *
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

   PROCEDURE prc_grant_tool (p_vc_schema VARCHAR2);

   PROCEDURE prc_revoke_tool (p_vc_schema VARCHAR2);

   PROCEDURE prc_grant_trac (p_vc_schema VARCHAR2);

   PROCEDURE prc_revoke_trac (p_vc_schema VARCHAR2);

   PROCEDURE prc_grant_mesr (p_vc_schema VARCHAR2);

   PROCEDURE prc_revoke_mesr (p_vc_schema VARCHAR2);

   PROCEDURE prc_grant_stag (p_vc_schema VARCHAR2);

   PROCEDURE prc_revoke_stag (p_vc_schema VARCHAR2);
END auth;
/

SHOW ERRORS