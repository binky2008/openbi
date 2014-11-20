CREATE OR REPLACE VIEW p#frm#taxn_v
AS
       SELECT LEVEL AS taxn_level
            , taxn_id
            , taxn_code
            , taxn_name
            , SYS_CONNECT_BY_PATH (
                 taxn_code
               , '/'
              )
                 taxn_path
         FROM p#frm#taxn_t
   START WITH taxn_parent_id IS NULL
   CONNECT BY PRIOR taxn_id = taxn_parent_id;