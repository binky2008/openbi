CREATE OR REPLACE PACKAGE p#frm#mesr
   AUTHID CURRENT_USER
AS
   /**
   *
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: mesr-def.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/mesr/mesr-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: mesr-def.sql 15 2015-05-03 16:17:11Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/mesr/mesr-def.sql $';
   /**
   * Package body version string.
   */
   c_body_version            VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                VARCHAR2 (1024);

   PROCEDURE prc_mesr_taxn_ins (
      p_vc_query_code      IN VARCHAR2
    , p_vc_taxonomy_code   IN VARCHAR2
   );

   PROCEDURE prc_mesr_taxn_del (
      p_vc_query_code      IN VARCHAR2
    , p_vc_taxonomy_code   IN VARCHAR2
   );

   PROCEDURE prc_query_ins (
      p_vc_query_code   IN VARCHAR2
    , p_vc_query_name   IN VARCHAR2
    , p_vc_query_sql    IN CLOB
   );

   PROCEDURE prc_query_del (
      p_vc_query_code   IN VARCHAR2
    , p_b_cascade       IN BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_keyfigure_ins (
      p_vc_query_code       IN VARCHAR2
    , p_vc_keyfigure_code   IN VARCHAR2
    , p_vc_keyfigure_name   IN VARCHAR2
   );

   PROCEDURE prc_keyfigure_del (
      p_vc_query_code       IN VARCHAR2
    , p_vc_keyfigure_code   IN VARCHAR2
    , p_b_cascade           IN BOOLEAN DEFAULT FALSE
   );

   PROCEDURE prc_threshold_ins (
      p_vc_query_code       IN VARCHAR2
    , p_vc_keyfigure_code   IN VARCHAR2
    , p_vc_threshold_type   IN VARCHAR2
    , p_n_threshold_min     IN NUMBER
    , p_n_threshold_max     IN NUMBER
    , p_d_threshold_from    IN DATE DEFAULT TO_DATE (
                                               '01011111'
                                             , 'ddmmyyyy'
                                            )
    , p_d_threshold_to      IN DATE DEFAULT TO_DATE (
                                               '09099999'
                                             , 'ddmmyyyy'
                                            )
   );

   PROCEDURE prc_exec_ins (
      p_vc_query_code       IN VARCHAR2
    , p_vc_keyfigure_code   IN VARCHAR2
    , p_n_result_value      IN NUMBER
    , p_vc_result_report    IN CLOB
   );

   PROCEDURE prc_exec (
      p_vc_query_code          IN VARCHAR2 DEFAULT 'ALL'
    , p_b_exception_if_fails   IN BOOLEAN DEFAULT FALSE
    , p_vc_storage_type        IN VARCHAR2 DEFAULT 'VALUE'
   );

   PROCEDURE prc_exec_taxonomy (
      p_vc_taxonomy_code       IN VARCHAR2
    , p_b_exception_if_fails   IN BOOLEAN DEFAULT FALSE
    , p_vc_storage_type        IN VARCHAR2 DEFAULT 'VALUE'
   );
END p#frm#mesr;