CREATE OR REPLACE PACKAGE p#frm#docu
   AUTHID CURRENT_USER
AS
   /**
   * Package containing general purpose functions and procedures
   *
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: docu-def.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/docu/docu-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: docu-def.sql 15 2015-05-03 16:17:11Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/docu/docu-def.sql $';
   /**
   * Package body version string.
   */
   c_body_version            VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                VARCHAR2 (1024);

   /**
   * Get stylesheet
   *
   * p_vc_stylesheet_type       Type of stylesheet
   */
   FUNCTION fct_get_stylesheet (p_vc_stylesheet_type VARCHAR2)
      RETURN CLOB;

   /**
   * Generate metadata item
   *
   * p_vc_content       Content to be transformed
   */
   FUNCTION fct_get_meta_item (p_vc_content VARCHAR2)
      RETURN CLOB;

   /**
   * Generate metadata part of the data set
   *
   * p_vc_content       Content to be transformed
   */
   FUNCTION fct_get_meta (p_vc_content CLOB)
      RETURN CLOB;

   /**
   * Generate a data cell
   *
   * p_vc_content       Content to be transformed
   */
   FUNCTION fct_get_data_cell (p_vc_content VARCHAR2)
      RETURN CLOB;

   /**
   * Generate data record
   *
   * p_vc_content       Content to be transformed
   */
   FUNCTION fct_get_data_record (p_vc_content CLOB)
      RETURN CLOB;

   /**
   * Generate data part of the data set
   *
   * p_vc_content       Content to be transformed
   */
   FUNCTION fct_get_data (p_vc_content CLOB)
      RETURN CLOB;

   /**
   * Generate complete dataset
   *
   * p_vc_content       Content to be transformed
   */
   FUNCTION fct_get_dataset (p_vc_content CLOB)
      RETURN CLOB;

   /**
   * Format dataset using a dataset and a style
   *
   * p_vc_content       Content to be transformed
   * p_vc_stylesheet    Stylesheet to transform the dataset in different output
   */
   FUNCTION fct_get_dataset_formatted (
      p_vc_dataset       CLOB
    , p_vc_stylesheet    CLOB
   )
      RETURN CLOB;

   /**
   * Generate report of given type from a document
   *
   * p_vc_document      Document to be put in the type template
   * p_vc_type          Type (html, excel)
   */
   FUNCTION fct_get_document (
      p_vc_content    CLOB
    , p_vc_type       CLOB
   )
      RETURN CLOB;

   /**
   * Get a report about the content of a given table in the wished format
   *
   * @param p_vc_table_name       Table name
   * @param p_vc_column_list      Column lists
   * @param p_vc_where_clause     Where clause
   * @param p_vc_report_format    Output format
   * @return                      Report object (table) in the chosen format
   */
   FUNCTION fct_get_table_dataset (
      p_vc_table_owner    IN VARCHAR2
    , p_vc_table_name     IN VARCHAR2
    , p_vc_column_list    IN VARCHAR2 DEFAULT NULL
    , p_vc_where_clause   IN VARCHAR2 DEFAULT NULL
    , p_vc_order_clause   IN VARCHAR2 DEFAULT NULL
   )
      RETURN CLOB;

   /**
   * Save a document in the aux_DOC table
   *
   * @param p_vc_docu_code      Document code
   * @param p_vc_docu_type      Document type
   * @param p_vc_docu_content   Document content
   * @param p_vc_docu_url      Document URL
   * @param p_vc_docu_desc      Document description
   */
   PROCEDURE prc_save_document (
      p_vc_docu_code      IN VARCHAR2
    , p_vc_docu_type      IN VARCHAR2
    , p_vc_docu_content   IN CLOB
    , p_vc_docu_url       IN VARCHAR2 DEFAULT NULL
    , p_vc_docu_desc      IN VARCHAR2 DEFAULT NULL
   );
END p#frm#docu;