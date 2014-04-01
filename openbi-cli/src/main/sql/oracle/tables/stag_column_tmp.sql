BEGIN
   ddls.prc_drop_object ('TABLE', 'stag_column_tmp');
END;
/

CREATE TABLE stag_column_tmp (
     stag_column_pos     NUMBER
   , stag_column_name    VARCHAR2 (100)
   , stag_column_comment VARCHAR2 (4000)
   , stag_column_def     VARCHAR2 (100)
   , stag_column_nk_pos  NUMBER
);


COMMENT ON TABLE stag_column_tmp IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stag_column_tmp TO PUBLIC;

BEGIN
   ddls.prc_create_synonym ('stag_column_tmp'
                                 , 'stag_column_tmp'
                                 , TRUE
                                  );
END;
/