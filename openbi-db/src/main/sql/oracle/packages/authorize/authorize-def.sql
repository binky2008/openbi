CREATE OR REPLACE PACKAGE authorize
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2011-07-19 17:32:56 +0200 (Di, 19 Jul 2011) $
    * $Revision: 1068 $
    * $Id: pkg_grant-def.sql 1068 2011-07-19 15:32:56Z nmarangoni $
    * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_grant/pkg_grant-def.sql $
    */

   /**
    * Package spec version string.
    */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: pkg_grant-def.sql 1068 2011-07-19 15:32:56Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_grant/pkg_grant-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version            VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                VARCHAR2 (1024);

   PROCEDURE prc_grant_utl (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_revoke_utl (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_grant_trc (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_revoke_trc (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_grant_mes (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_revoke_mes (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_grant_stg (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_revoke_stg (
      p_vc_schema   VARCHAR2
   );
END authorize;
/

SHOW errors