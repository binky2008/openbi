BEGIN
   ddl.prc_drop_object ('TABLE', 'stg_column_tmp');
END;
/

CREATE TABLE stg_column_tmp (
     stg_column_pos     NUMBER
   , stg_column_name    VARCHAR2 (100)
   , stg_column_comment VARCHAR2 (4000)
   , stg_column_def     VARCHAR2 (100)
   , stg_column_nk_pos  NUMBER
);


COMMENT ON TABLE stg_column_tmp IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stg_column_tmp TO PUBLIC;

BEGIN
   ddl.prc_create_synonym ('stg_column_tmp'
                                 , 'stg_column_tmp'
                                 , TRUE
                                  );
END;
/