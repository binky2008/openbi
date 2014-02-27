BEGIN
   utl_ddl.prc_drop_object ('TABLE', 'etl_stage_column_tmp');
END;
/

CREATE TABLE etl_stage_column_tmp (
     etl_stage_column_pos     NUMBER
   , etl_stage_column_name    VARCHAR2 (100)
   , etl_stage_column_comment VARCHAR2 (4000)
   , etl_stage_column_def     VARCHAR2 (100)
   , etl_stage_column_nk_pos  NUMBER
);


COMMENT ON TABLE etl_stage_column_tmp IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON etl_stage_column_tmp TO PUBLIC;

BEGIN
   utl_ddl.prc_create_synonym ('etl_stage_column_tmp'
                                 , 'etl_stage_column_tmp'
                                 , TRUE
                                  );
END;
/