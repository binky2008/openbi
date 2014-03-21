CREATE OR REPLACE PACKAGE enable authid current_user
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2011-07-19 17:32:56 +0200 (Di, 19 Jul 2011) $
    * $Revision: 1068 $
    * $Id: pkg_enable-def.sql 1068 2011-07-19 15:32:56Z nmarangoni $
    * $HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_enable/pkg_enable-def.sql $
    */

   /**
    * Package spec version string.
    */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: pkg_enable-def.sql 1068 2011-07-19 15:32:56Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwp1466/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_enable/pkg_enable-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version            VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                VARCHAR2 (1024);

   PROCEDURE prc_enable_utl (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_disable_utl (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_enable_trc (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_disable_trc (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_enable_mes (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_disable_mes (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_enable_stg (
      p_vc_schema   VARCHAR2
   );

   PROCEDURE prc_disable_stg (
      p_vc_schema   VARCHAR2
   );
END enable;
/

SHOW errors