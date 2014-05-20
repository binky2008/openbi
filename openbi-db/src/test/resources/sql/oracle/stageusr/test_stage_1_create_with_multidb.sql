BEGIN
   ROLLBACK;
   p#frm#stag_meta.prc_source_ins (
      'SGM'
    , 'SGM'
    , 'SugarCRM Multiple'
    , 'DWHSTAGE'
    , 'USERS'
    , 'USERS'
    , 'USERS'
    , 'USERS'
   );
   --
   p#frm#stag_meta.prc_source_db_ins (
      'SGM'
    , 'SG1'
    , 'SUGARCRM'
    , 'SUGARCRM'
   );
   p#frm#stag_meta.prc_source_db_ins (
      'SGM'
    , 'SG2'
    , 'SUGARCRM'
    , 'SUGARCRM'
   );
   p#frm#stag_meta.prc_object_ins (
      'SGM'
    , 'ACCOUNTS'
   );
   p#frm#stag_meta.prc_object_ins (
      'SGM'
    , 'ACL_ACTIONS'
   );
   --
   p#frm#stag_meta.prc_column_import_from_source ('SGM');
   --
   p#frm#stag_meta.prc_column_ins (
      'SGM'
    , 'ACCOUNTS'
    , 'DESCRIPTION'
    , p_n_column_edwh_flag   => 0
   );
   --
   p#frm#stag_meta.prc_column_ins (
      'SGM'
    , 'ACL_ACTIONS'
    , 'ID'
    , p_n_column_nk_pos   => 1
   );
   --
   p#frm#stag_build.prc_build_all ('SGM');
   ROLLBACK;
END;