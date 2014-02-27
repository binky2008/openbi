CREATE OR REPLACE PACKAGE aux_doc AUTHID CURRENT_USER AS
  /**
   * Package containing general purpose functions and procedures
   *
  * $Author: nmarangoni $
  * $Date: $
  * $Revision: $
  * $Id: $
  * $HeadURL: $
   */

  /**
  * Package spec version string.
  */
  c_spec_version CONSTANT VARCHAR2(1024) := '$Id: $';
  /**
  * Package spec repository URL.
  */
  c_spec_url CONSTANT VARCHAR2(1024) := '$HeadURL: $';
  /**
  * Package body version string.
  */
  c_body_version VARCHAR2(1024);
  /**
  * Package body repository URL.
  */
  c_body_url VARCHAR2(1024);
  /**
  * Package name
  */
  c_pkg_name CONSTANT VARCHAR2(50) := 'aux_report';

  /**
  * Get stylesheet
  *
  * p_vc_stylesheet_type       Type of stylesheet
  */
  FUNCTION fct_get_stylesheet(p_vc_stylesheet_type VARCHAR2) RETURN CLOB;

  /**
  * Generate metadata item
  *
  * p_vc_content       Content to be transformed
  */
  FUNCTION fct_get_meta_item(p_vc_content VARCHAR2) RETURN CLOB;

  /**
  * Generate metadata part of the data set
  *
  * p_vc_content       Content to be transformed
  */
  FUNCTION fct_get_meta(p_vc_content CLOB) RETURN CLOB;

  /**
  * Generate a data cell
  *
  * p_vc_content       Content to be transformed
  */
  FUNCTION fct_get_data_cell(p_vc_content VARCHAR2) RETURN CLOB;

  /**
  * Generate data record
  *
  * p_vc_content       Content to be transformed
  */
  FUNCTION fct_get_data_record(p_vc_content CLOB) RETURN CLOB;

  /**
  * Generate data part of the data set
  *
  * p_vc_content       Content to be transformed
  */
  FUNCTION fct_get_data(p_vc_content CLOB) RETURN CLOB;

  /**
  * Generate data part of the data set
  *
  * p_vc_content       Content to be transformed
  */
  FUNCTION fct_get_data(p_l_content aux_type.l_line_array) RETURN CLOB;

  /**
  * Generate complete dataset
  *
  * p_vc_content       Content to be transformed
  */
  FUNCTION fct_get_dataset(p_vc_content CLOB) RETURN CLOB;

  /**
  * Format dataset using a dataset and a style
  *
  * p_vc_content       Content to be transformed
  * p_vc_stylesheet    Stylesheet to transform the dataset in different output
  */
  FUNCTION fct_get_dataset_formatted(p_vc_dataset    CLOB
                                    ,p_vc_stylesheet CLOB) RETURN CLOB;

  /**
  * Generate report of given type from a document
  *
  * p_vc_document      Document to be put in the type template
  * p_vc_type          Type (html, excel)
  */
  FUNCTION fct_get_document(p_vc_content CLOB
                           ,p_vc_type    CLOB) RETURN CLOB;

  /**
  * Get a report about the content of a given table in the wished format
  *
  * @param p_vc_table_name       Table name
  * @param p_vc_column_list      Column lists
  * @param p_vc_where_clause     Where clause
  * @param p_vc_report_format    Output format
  * @return                      Report object (table) in the chosen format
  */
  FUNCTION fct_get_table_dataset(p_vc_table_owner  IN VARCHAR2
                                ,p_vc_table_name   IN VARCHAR2
                                ,p_vc_column_list  IN VARCHAR2 DEFAULT NULL
                                ,p_vc_where_clause IN VARCHAR2 DEFAULT NULL
                                ,p_vc_order_clause IN VARCHAR2 DEFAULT NULL)
    RETURN CLOB;

  /**
  * Save a document in the aux_DOC table
  *
  * @param p_vc_doc_code      Document code
  * @param p_vc_doc_type      Document type
  * @param p_vc_doc_content   Document content
  * @param p_vc_doc_url      Document URL
  * @param p_vc_doc_desc      Document description
  */
  PROCEDURE prc_save_document(p_vc_doc_code    IN VARCHAR2
                             ,p_vc_doc_type    IN VARCHAR2
                             ,p_vc_doc_content IN CLOB
                             ,p_vc_doc_url     IN VARCHAR2 DEFAULT NULL
                             ,p_vc_doc_desc    IN VARCHAR2 DEFAULT NULL);
END aux_doc;
/
