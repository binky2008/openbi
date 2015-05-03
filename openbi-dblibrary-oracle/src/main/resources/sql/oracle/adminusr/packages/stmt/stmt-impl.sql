CREATE OR REPLACE PACKAGE BODY p#frm#stmt
AS
   PROCEDURE prc_set_text_param (
      p_vc_code_string   IN OUT CLOB
    , p_vc_param_name    IN     VARCHAR2
    , p_vc_param_value   IN     CLOB
   )
   IS
      l_vc_buffer_in       CLOB;
      l_vc_buffer_out      CLOB;
      l_vc_token           CLOB;
      l_i_position_begin   INTEGER;
      l_i_position_end     INTEGER;
   BEGIN
      l_vc_buffer_in := p_vc_code_string;
      l_i_position_begin :=
           INSTR (
              l_vc_buffer_in
            ,    '#'
              || p_vc_param_name
              || '#'
           )
         - 1;
      l_i_position_end :=
           INSTR (
              l_vc_buffer_in
            ,    '#'
              || p_vc_param_name
              || '#'
           )
         + LENGTH (p_vc_param_name)
         + 2;

      -- Loop on occurencies of the parameter into the root code
      WHILE l_i_position_begin >= 0 LOOP
         l_vc_token :=
            SUBSTR (
               l_vc_buffer_in
             , 1
             , l_i_position_begin
            );
         l_vc_buffer_out :=
               l_vc_buffer_out
            || l_vc_token;
         l_vc_buffer_out :=
               l_vc_buffer_out
            || p_vc_param_value;
         l_vc_buffer_in :=
            SUBSTR (
               l_vc_buffer_in
             , l_i_position_end
            );
         l_i_position_begin :=
              INSTR (
                 l_vc_buffer_in
               ,    '#'
                 || p_vc_param_name
                 || '#'
              )
            - 1;
         l_i_position_end :=
              INSTR (
                 l_vc_buffer_in
               ,    '#'
                 || p_vc_param_name
                 || '#'
              )
            + LENGTH (p_vc_param_name)
            + 2;
      END LOOP;

      -- Append the rest token
      l_vc_buffer_out :=
            l_vc_buffer_out
         || l_vc_buffer_in;
      p_vc_code_string := l_vc_buffer_out;
   EXCEPTION
      WHEN OTHERS THEN
         RAISE;
   END prc_set_text_param;
/**
 * Package initialization
 */
BEGIN
   c_body_version := '$Id: stmt-impl.sql 15 2015-05-03 16:17:11Z admin $';
   c_body_url := '$HeadURL: http://192.168.178.61/svn/odk/oracle/adminusr/packages/stmt/stmt-impl.sql $';
END p#frm#stmt;