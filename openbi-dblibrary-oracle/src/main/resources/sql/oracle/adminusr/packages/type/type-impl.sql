CREATE OR REPLACE PACKAGE BODY p#frm#type
AS
   /**
   * $Author: admin $
   * $Date: 2015-05-03 18:17:11 +0200 (So, 03 Mai 2015) $
   * $Revision: 15 $
   * $Id: type-impl.sql 15 2015-05-03 16:17:11Z admin $
   * $HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/type/type-impl.sql $
   */
   /**
   * Max length of a pl/sql code block
   */
   c_i_max_plsql_length   CONSTANT INTEGER := 32000;
   /**
   * Length of a varchar2s row
   */
   c_i_max_vc2s_length    CONSTANT INTEGER := 255;

   /**
   * String type for PL/SQL statements
   */
   SUBTYPE t_string IS VARCHAR2 (32767);

   PROCEDURE prc_check_state
   IS
   BEGIN
      NULL;
   END prc_check_state;

   FUNCTION fct_bool_to_flag (p_bool BOOLEAN)
      RETURN CHAR
   IS
      v_yn   CHAR;
   BEGIN
      v_yn :=
         CASE p_bool
            WHEN TRUE THEN
               'Y'
            WHEN FALSE THEN
               'N'
            ELSE
               NULL
         END;
      RETURN v_yn;
   END fct_bool_to_flag;

   FUNCTION fct_flag_to_bool (p_char CHAR)
      RETURN BOOLEAN
   IS
      v_bool   BOOLEAN;
   BEGIN
      v_bool :=
         CASE p_char
            WHEN 'Y' THEN
               TRUE
            WHEN 'N' THEN
               FALSE
            ELSE
               NULL
         END;
      RETURN v_bool;
   END fct_flag_to_bool;

   FUNCTION fct_bool_to_string (p_bool BOOLEAN)
      RETURN VARCHAR2
   IS
      v_str   VARCHAR2 (5);
   BEGIN
      v_str :=
         CASE p_bool
            WHEN TRUE THEN
               'TRUE'
            WHEN FALSE THEN
               'FALSE'
            ELSE
               NULL
         END;
      RETURN v_str;
   END fct_bool_to_string;

   FUNCTION fct_string_to_bool (p_str VARCHAR2)
      RETURN BOOLEAN
   IS
      v_bool   BOOLEAN;
   BEGIN
      v_bool :=
         CASE p_str
            WHEN 'TRUE' THEN
               TRUE
            WHEN 'FALSE' THEN
               FALSE
            ELSE
               NULL
         END;
      RETURN v_bool;
   END fct_string_to_bool;

   FUNCTION fct_list_to_clob (
      p_str_list      DBMS_SQL.varchar2s
    , p_vc_separer    VARCHAR2 DEFAULT ','
   )
      RETURN CLOB
   IS
      v_out_clob   CLOB;
   BEGIN
      IF p_str_list.COUNT > 0 THEN
         FOR i IN p_str_list.FIRST .. p_str_list.LAST LOOP
            IF i > 1 THEN
               v_out_clob :=
                     v_out_clob
                  || p_vc_separer;
            END IF;

            v_out_clob :=
                  v_out_clob
               || p_str_list (i);
         END LOOP;
      END IF;

      RETURN v_out_clob;
   END fct_list_to_clob;

   FUNCTION fct_clob_to_list (
      p_cclob         CLOB
    , p_vc_separer    VARCHAR2 DEFAULT ','
   )
      RETURN DBMS_SQL.varchar2s
   IS
      v_cclob      CLOB;
      v_vcline     t_string;
      v_ilf        INTEGER;
      v_out_list   DBMS_SQL.varchar2s;
   BEGIN
      -- eliminate CHAR(13) chars, keep only CHAR(10)
      v_cclob :=
         REPLACE (
            p_cclob
          , CHR (13)
         );

      LOOP
         v_ilf :=
            NVL (
               INSTR (
                  v_cclob
                , p_vc_separer
               )
             , 0
            );

         IF v_ilf = 0 THEN
            v_vcline := v_cclob;
         ELSE
            v_vcline :=
               SUBSTR (
                  v_cclob
                , 1
                ,   v_ilf
                  - 1
               );
            v_cclob :=
               SUBSTR (
                  v_cclob
                ,   v_ilf
                  + 1
               );
         END IF;

         -- write new line to
         v_out_list (NVL (
                          v_out_list.LAST
                        + 1
                      , 1
                     )) :=
            v_vcline;
         EXIT WHEN v_ilf = 0;
      END LOOP;

      RETURN v_out_list;
   END fct_clob_to_list;

   FUNCTION fct_string_to_list (
      p_vcstring      VARCHAR2
    , p_vc_separer    VARCHAR2 DEFAULT ','
   )
      RETURN DBMS_SQL.varchar2s
   IS
      v_vcstring   t_string;
      v_vcline     t_string;
      v_ilf        INTEGER;
      v_out_list   DBMS_SQL.varchar2s;
   BEGIN
      -- eliminate CHAR(13) chars, keep only CHAR(10)
      v_vcstring :=
         REPLACE (
            p_vcstring
          , CHR (13)
         );

      LOOP
         v_ilf :=
            NVL (
               INSTR (
                  v_vcstring
                , p_vc_separer
               )
             , 0
            );

         IF v_ilf = 0 THEN
            v_vcline := v_vcstring;
         ELSE
            v_vcline :=
               SUBSTR (
                  v_vcstring
                , 1
                ,   v_ilf
                  - 1
               );
            v_vcstring :=
               SUBSTR (
                  v_vcstring
                ,   v_ilf
                  + 1
               );
         END IF;

         v_out_list (NVL (
                          v_out_list.LAST
                        + 1
                      , 1
                     )) :=
            v_vcline;
         EXIT WHEN v_ilf = 0;
      END LOOP;

      RETURN v_out_list;
   END fct_string_to_list;

   FUNCTION fct_list_to_string (
      p_vc2string     DBMS_SQL.varchar2s
    , p_vc_separer    VARCHAR2 DEFAULT ','
   )
      RETURN VARCHAR2
   IS
      v_max_cnt   INTEGER := 0;
      v_str       t_string;
   BEGIN
      v_max_cnt :=
         FLOOR (  c_i_max_plsql_length
                / (  c_i_max_vc2s_length
                   + 1));

      IF p_vc2string.COUNT > 0 THEN
      FOR idx IN p_vc2string.FIRST .. LEAST(p_vc2string.COUNT
                                           ,v_max_cnt) LOOP
        IF idx > 1 THEN
          v_str := v_str || p_vc_separer;
        END IF;

        v_str := v_str || p_vc2string(idx);
      END LOOP;
      END IF;

      RETURN v_str;
   END fct_list_to_string;

   FUNCTION fct_format_str_array (p_str_array DBMS_SQL.varchar2s)
      RETURN VARCHAR2
   IS
      v_max_cnt   INTEGER := 0;
      v_str       t_string;
   BEGIN
      v_max_cnt :=
         FLOOR (  c_i_max_plsql_length
                / (  c_i_max_vc2s_length
                   + 1));

      IF p_str_array.COUNT > 0 THEN
      FOR idx IN p_str_array.FIRST .. LEAST(p_str_array.COUNT
                                           ,v_max_cnt) LOOP
        v_str := v_str || p_str_array(idx) || CHR(10);
      END LOOP;
      END IF;

      RETURN v_str;
   END fct_format_str_array;

   -- get max line length
   FUNCTION fct_get_max_line_length (p_vcstring VARCHAR2)
      RETURN INTEGER
   IS
      v_vcstring   t_string;
      v_vcline     t_string;
      v_ilf        INTEGER;
      v_imaxlen    INTEGER := 0;
   BEGIN
      -- eliminate CHAR(13) chars, keep only CHAR(10)
      v_vcstring :=
         REPLACE (
            p_vcstring
          , CHR (13)
         );

      LOOP
         v_ilf :=
            NVL (
               INSTR (
                  v_vcstring
                , CHR (10)
               )
             , 0
            );

         IF v_ilf = 0 THEN
            v_vcline := v_vcstring;
         ELSE
            v_vcline :=
               SUBSTR (
                  v_vcstring
                , 1
                ,   v_ilf
                  - 1
               );
            v_vcstring :=
               SUBSTR (
                  v_vcstring
                ,   v_ilf
                  + 1
               );
         END IF;

         -- preserve the maximum line length
         v_imaxlen :=
            GREATEST (
               v_imaxlen
             , NVL (LENGTH (v_vcline), 0)
            );
         EXIT WHEN v_ilf = 0;
      END LOOP;

      RETURN v_imaxlen;
   END fct_get_max_line_length;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: type-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/type/type-impl.sql $';
END p#frm#type;