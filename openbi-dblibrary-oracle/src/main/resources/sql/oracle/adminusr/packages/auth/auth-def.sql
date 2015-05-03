CREATE OR REPLACE PACKAGE p#frm#auth
AS
   /**
   *
   *
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: auth-def.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/auth/auth-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: auth-def.sql 15 2015-05-03 16:17:11Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/auth/auth-def.sql $';
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
END p#frm#auth;