CREATE OR REPLACE PACKAGE p#frm#taxn
   AUTHID CURRENT_USER
AS
   /**
   *
   * $Author: admin $
   * $Date: 2015-05-03 17:35:13 +0200 (So, 03 Mai 2015) $
   * $Revision: 5 $
   * $Id: taxn-def.sql 5 2015-05-03 15:35:13Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/taxn/taxn-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: taxn-def.sql 5 2015-05-03 15:35:13Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/taxn/taxn-def.sql $';
   /**
   * Package body version string.
   */
   c_body_version            VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                VARCHAR2 (1024);

   FUNCTION fct_get_taxonomy_emails (
      p_vc_taxonomy_code   IN VARCHAR2
    , p_vc_separator       IN VARCHAR2 DEFAULT ','
   )
      RETURN VARCHAR2;

   PROCEDURE prc_taxonomy_ins (
      p_vc_taxonomy_code          IN VARCHAR2
    , p_vc_taxonomy_name          IN VARCHAR2
    , p_vc_taxonomy_parent_code   IN VARCHAR2
   );

   PROCEDURE prc_user_ins (
      p_vc_user_code    IN VARCHAR2
    , p_vc_user_name    IN VARCHAR2
    , p_vc_user_email   IN VARCHAR2
   );

   PROCEDURE prc_user_taxonomy_ins (
      p_vc_user_code       IN VARCHAR2
    , p_vc_taxonomy_code   IN VARCHAR2
   );

   PROCEDURE prc_user_taxonomy_del (
      p_vc_user_code       IN VARCHAR2
    , p_vc_taxonomy_code   IN VARCHAR2
   );
END p#frm#taxn;