MERGE INTO p#frm#taxn_t trg
     USING (SELECT 'GLOBAL' AS taxn_code FROM DUAL) src
        ON (trg.taxn_code = src.taxn_code)
WHEN NOT MATCHED THEN
   INSERT     (
                 taxn_id
               , taxn_code
               , taxn_name
              )
       VALUES (
                 -1
               , 'GLOBAL'
               , 'Global'
              );
COMMIT;