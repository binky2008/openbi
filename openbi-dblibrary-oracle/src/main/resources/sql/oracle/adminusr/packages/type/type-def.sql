CREATE OR REPLACE PACKAGE p#frm#type
AS
   /**
   * Package containing standard types and type conversion functions
   *
   *
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: type-def.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/type/type-def.sql $
   */
   /**
   * Package spec version string.
   */
   c_spec_version   CONSTANT VARCHAR2 (1024) := '$Id: type-def.sql 15 2015-05-03 16:17:11Z admin $';
   /**
   * Package spec repository URL.
   */
   c_spec_url       CONSTANT VARCHAR2 (1024) := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/type/type-def.sql $';
   /**
   * Package body version string.
   */
   c_body_version            VARCHAR2 (1024);
   /**
   * Package body repository URL.
   */
   c_body_url                VARCHAR2 (1024);

   /**
   * Dummy test procedure to fix package state issue
   */
   PROCEDURE prc_check_state;

   /**
   * Boolean to Y/N flag
   *
   * @param p_bool     Boolean value
   * @return           'Y' or 'N'
   */
   FUNCTION fct_bool_to_flag (p_bool BOOLEAN)
      RETURN CHAR;

   /**
   * Y/N to boolean
   *
   * @param p_char     'Y' or 'N'
   * @return           Boolean value
   */
   FUNCTION fct_flag_to_bool (p_char CHAR)
      RETURN BOOLEAN;

   /**
   * Boolean to Y/N flag
   *
   * @param p_bool     Boolean value
   * @return           Input value as string
   */
   FUNCTION fct_bool_to_string (p_bool BOOLEAN)
      RETURN VARCHAR2;

   /**
   * Y/N to boolean
   *
   * @param p_str      'TRUE' or 'FALSE' as a string
   * @return           Boolean value
   */
   FUNCTION fct_string_to_bool (p_str VARCHAR2)
      RETURN BOOLEAN;

   /**
   * Convert a VARCHAR2S text list to a clob
   *
   * @param p_str_list   List of strings
   * @return             Clob containing the formatted list
   */
   FUNCTION fct_list_to_clob (
      p_str_list      DBMS_SQL.varchar2s
    , p_vc_separer    VARCHAR2 DEFAULT ','
   )
      RETURN CLOB;

   /**
   * Convert a CLOB text to a VARCHAR2S array, use line breaks to separate the rows
   *
   * @param p_cclob       Input clob
   * @param p_vc_separer  Separer string
   * @return              list containing the formatted content of the clob
   */
   FUNCTION fct_clob_to_list (
      p_cclob         CLOB
    , p_vc_separer    VARCHAR2 DEFAULT ','
   )
      RETURN DBMS_SQL.varchar2s;

   /**
   * Convert a VARCHAR text to a VARCHAR2S array, use line breaks to separate the rows
   *
   * @param p_vcstring   Input string
   * @param p_vc_separer  Separer string
   * @return             List containing the formatted list
   */
   FUNCTION fct_string_to_list (
      p_vcstring      VARCHAR2
    , p_vc_separer    VARCHAR2 DEFAULT ','
   )
      RETURN DBMS_SQL.varchar2s;

   /**
   * Convert a VARCHAR2S array to a VARCHAR2 string, use line breaks to separate the rows
   *
   * @param p_vcstring   Input list
   * @param p_vc_separer  Separer string
   * @return             String containing the formatted list
   */
   FUNCTION fct_list_to_string (
      p_vc2string     DBMS_SQL.varchar2s
    , p_vc_separer    VARCHAR2 DEFAULT ','
   )
      RETURN VARCHAR2;

   /**
   * Format VARCHAR2S for debug output
   *
   * @param p_str_array  Array of strings
   * @return             String containing the formatted list
   */
   FUNCTION fct_format_str_array (p_str_array DBMS_SQL.varchar2s)
      RETURN VARCHAR2;

   /**
   * Get max line length
   *
   * @param p_vcstring       String to check
   * @return                 Line length
   */
   FUNCTION fct_get_max_line_length (p_vcstring VARCHAR2)
      RETURN INTEGER;
END p#frm#type;