CREATE OR REPLACE PACKAGE stag_stat
   AUTHID CURRENT_USER
AS
   /**
   * Package containing tools to collect statistics and size of STAGE tables
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

   /**
    * Set global load id
    */
   --PROCEDURE prc_set_load_id;
   /**
    * Create a synonym for a given object
    *
    * @param p_vc_source_code       Source name
    * @param p_vc_object_name       Object name
    * @param p_n_partition          Table partition
    * @param p_vc_stat_type_code    Statistics type
    */
   FUNCTION prc_stat_begin (
      p_vc_source_code       VARCHAR2
    , p_vc_object_name       VARCHAR2
    , p_n_partition          NUMBER DEFAULT NULL
    , p_vc_stat_type_code    VARCHAR2 DEFAULT NULL
   )
      RETURN NUMBER;

   PROCEDURE prc_stat_end (
      p_n_stat_id       NUMBER
    , p_n_stat_value    NUMBER DEFAULT 0
    , p_n_stat_error    NUMBER DEFAULT 0
   );

   PROCEDURE prc_stat_purge;

   PROCEDURE prc_size_store (
      p_vc_source_code    VARCHAR2
    , p_vc_object_name    VARCHAR2
    , p_vc_table_name     VARCHAR2
   );
END stag_stat;