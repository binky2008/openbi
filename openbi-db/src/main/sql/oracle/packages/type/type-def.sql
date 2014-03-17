CREATE OR REPLACE PACKAGE type AS
  /**
   * Package containing standard types and type conversion functions
   *
  * $Author: nmarangoni $
  * $Date: $
  * $Revision: $
  * $Id: $
  * $HeadURL: $
   */
  /**
  * Package name
  */
  c_pkg_name CONSTANT VARCHAR2(50) := 'pkg_aux_type';
  /**
  * Bad parameter exception number
  */
  c_de_bad_param CONSTANT NUMBER := -20002;
  /**
  * ORA-20002 - Bad parameter
  */
  e_de_bad_param EXCEPTION;
  PRAGMA EXCEPTION_INIT(e_de_bad_param
                       ,-20002);
  /**
  * Max length of a pl/sql code block
  */
  c_i_max_plsql_length CONSTANT INTEGER := 32000;
  /**
  * Length of a varchar2s row
  */
  c_i_max_vc2s_length CONSTANT INTEGER := 255;

  /**
  * String type for object names
  */
  SUBTYPE vc_obj_plsql IS VARCHAR2(100);

  /**
  * String type for object names
  */
  SUBTYPE vc_max_line IS VARCHAR2(255);

  /**
  * String type for PL/SQL statements
  */
  SUBTYPE vc_max_plsql IS VARCHAR2(32000);

  /**
  * String type for table columns
  */
  SUBTYPE vc_max_db IS VARCHAR2(4000);

  /**
  * List of program lines
  */
  TYPE l_line_array IS TABLE OF vc_max_db INDEX BY PLS_INTEGER;

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
  FUNCTION fct_bool_to_flag(p_bool BOOLEAN) RETURN CHAR;

  /**
  * Y/N to boolean
  *
  * @param p_char     'Y' or 'N'
  * @return           Boolean value
  */
  FUNCTION fct_flag_to_bool(p_char CHAR) RETURN BOOLEAN;

  /**
  * Boolean to Y/N flag
  *
  * @param p_bool     Boolean value
  * @return           Input value as string
  */
  FUNCTION fct_bool_to_string(p_bool BOOLEAN) RETURN VARCHAR2;

  /**
  * Y/N to boolean
  *
  * @param p_str      'TRUE' or 'FALSE' as a string
  * @return           Boolean value
  */
  FUNCTION fct_string_to_bool(p_str VARCHAR2) RETURN BOOLEAN;

  /**
  * Convert a VARCHAR2S text list to a clob
  *
  * @param p_str_list   List of strings
  * @return             Clob containing the formatted list
  */
  FUNCTION fct_list_to_clob(p_str_list   dbms_sql.varchar2s
                           ,p_vc_separer VARCHAR2 DEFAULT chr(10))
    RETURN CLOB;

  /**
  * Convert a CLOB text to a VARCHAR2S array, use line breaks to separate the rows
  *
  * @param p_cclob       Input clob
  * @param p_vc_separer  Separer string
  * @return              list containing the formatted content of the clob
  */
  FUNCTION fct_clob_to_list(p_cclob      CLOB
                           ,p_vc_separer VARCHAR2 DEFAULT chr(10))
    RETURN dbms_sql.varchar2s;

  /**
  * Convert a VARCHAR text to a VARCHAR2S array, use line breaks to separate the rows
  *
  * @param p_vcstring   Input string
  * @param p_vc_separer  Separer string
  * @return             List containing the formatted list
  */
  FUNCTION fct_string_to_list(p_vcstring   VARCHAR2
                             ,p_vc_separer VARCHAR2 DEFAULT chr(10))
    RETURN dbms_sql.varchar2s;

  /**
  * Convert a VARCHAR2S array to a VARCHAR2 string, use line breaks to separate the rows
  *
  * @param p_vcstring   Input list
  * @param p_vc_separer  Separer string
  * @return             String containing the formatted list
  */
  FUNCTION fct_list_to_string(p_vc2string  dbms_sql.varchar2s
                             ,p_vc_separer VARCHAR2 DEFAULT chr(10))
    RETURN VARCHAR2;

  /**
  * Format VARCHAR2S for debug output
  *
  * @param p_str_array  Array of strings
  * @return             String containing the formatted list
  */
  FUNCTION fct_format_str_array(p_str_array dbms_sql.varchar2s)
    RETURN VARCHAR2;

  /**
  * Get max line length
  *
  * @param p_vcstring       String to check
  * @return                 Line length
  */
  FUNCTION fct_get_max_line_length(p_vcstring VARCHAR2) RETURN INTEGER;
END type;
/
