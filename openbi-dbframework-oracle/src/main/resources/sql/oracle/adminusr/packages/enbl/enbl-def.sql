CREATE OR REPLACE PACKAGE p#frm#enbl
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

   PROCEDURE prc_enable_tool (p_vc_tools_owner IN VARCHAR2);

   PROCEDURE prc_disable_tool;

   PROCEDURE prc_enable_trac (p_vc_tools_owner IN VARCHAR2);

   PROCEDURE prc_disable_trac;

   PROCEDURE prc_enable_mesr (p_vc_tools_owner IN VARCHAR2);

   PROCEDURE prc_disable_mesr;

   PROCEDURE prc_enable_stag (p_vc_tools_owner IN VARCHAR2);

   PROCEDURE prc_disable_stag;
END p#frm#enbl;