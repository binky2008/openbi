CREATE OR REPLACE PACKAGE txn_taxonomy AUTHID CURRENT_USER
AS
   /**
    * $Author: nmarangoni $
    * $Date: 2012-03-20 09:36:48 +0100 (Di, 20 Mrz 2012) $
    * $Revision: 2482 $
    * $Id: pkg_sys-def.sql 2482 2012-03-20 08:36:48Z nmarangoni $
    * $HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_sys/pkg_sys-def.sql $
    */

   /**
    * Package spec version string.
    */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: pkg_sys-def.sql 2482 2012-03-20 08:36:48Z nmarangoni $';
   /**
    * Package spec repository URL.
    */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: svn://qwd4067/svn_repository_bic/edwh/dwso/edwh_adm/packages/pkg_sys/pkg_sys-def.sql $';
   /**
    * Package body version string.
    */
   c_body_version            VARCHAR2 (1024);
   /**
    * Package body repository URL.
    */
   c_body_url                VARCHAR2 (1024);

   FUNCTION fct_get_taxonomy_emails (
      p_vc_taxonomy_code   IN   VARCHAR2
    , p_vc_separator       IN   VARCHAR2 DEFAULT ','
   )
      RETURN VARCHAR2;

   PROCEDURE prc_environment_ins (
      p_vc_environment_code   IN   VARCHAR2
    , p_vc_environment_name   IN   VARCHAR2
    , p_vc_environment_db     IN   VARCHAR2
   );

   PROCEDURE prc_layer_ins (
      p_vc_layer_code   IN   VARCHAR2
    , p_vc_layer_name   IN   VARCHAR2
   );

   PROCEDURE prc_entity_ins (
      p_vc_entity_code   IN   VARCHAR2
    , p_vc_entity_name   IN   VARCHAR2
   );

   PROCEDURE prc_taxonomy_ins (
      p_vc_taxonomy_code          IN   VARCHAR2
    , p_vc_taxonomy_name          IN   VARCHAR2
    , p_vc_taxonomy_parent_code   IN   VARCHAR2
   );

   PROCEDURE prc_user_ins (
      p_vc_user_code    IN   VARCHAR2
    , p_vc_user_name    IN   VARCHAR2
    , p_vc_user_email   IN   VARCHAR2
   );

   PROCEDURE prc_user_taxonomy_ins (
      p_vc_user_code       IN   VARCHAR2
    , p_vc_taxonomy_code   IN   VARCHAR2
   );

   PROCEDURE prc_user_taxonomy_del (
      p_vc_user_code       IN   VARCHAR2
    , p_vc_taxonomy_code   IN   VARCHAR2
   );
END txn_taxonomy;
/

SHOW errors