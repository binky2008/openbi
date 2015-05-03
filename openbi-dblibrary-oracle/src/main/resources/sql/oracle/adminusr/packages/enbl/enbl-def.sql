CREATE OR REPLACE PACKAGE p#frm#enbl
   AUTHID CURRENT_USER
AS
   /**
   *
   * $Author: admin $
   * $Date: 2015-05-03 17:35:13 +0200 (So, 03 Mai 2015) $
   * $Revision: 5 $
   * $Id: enbl-def.sql 5 2015-05-03 15:35:13Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/enbl/enbl-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: enbl-def.sql 5 2015-05-03 15:35:13Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/enbl/enbl-def.sql $';
   /**
   * Package body version string.
   */
   c_body_version            VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                VARCHAR2 (1024);

   PROCEDURE prc_enable_tool (p_vc_tools_owner IN VARCHAR2);

   PROCEDURE prc_disable_tool;

   PROCEDURE prc_enable_trac (p_vc_tools_owner IN VARCHAR2);

   PROCEDURE prc_disable_trac;

   PROCEDURE prc_enable_mesr (p_vc_tools_owner IN VARCHAR2);

   PROCEDURE prc_disable_mesr;

   PROCEDURE prc_enable_stag (p_vc_tools_owner IN VARCHAR2);

   PROCEDURE prc_disable_stag;
END p#frm#enbl;