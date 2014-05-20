BEGIN
   ROLLBACK;
   frm_stag_meta.prc_source_ins (
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
   frm_stag_meta.prc_source_db_ins (
      'SGM'
    , 'SG1'
    , 'SUGARCRM'
    , 'SUGARCRM'
   );
   frm_stag_meta.prc_source_db_ins (
      'SGM'
    , 'SG2'
    , 'SUGARCRM'
    , 'SUGARCRM'
   );
   frm_stag_meta.prc_object_ins (
      'SGM'
    , 'ACCOUNTS'
   );
   frm_stag_meta.prc_object_ins (
      'SGM'
    , 'ACL_ACTIONS'
   );
   --
   frm_stag_meta.prc_column_import_from_source ('SGM');
   --
   frm_stag_meta.prc_column_ins (
      'SGM'
    , 'ACCOUNTS'
    , 'DESCRIPTION'
    , p_n_column_edwh_flag   => 0
   );
   --
   frm_stag_meta.prc_column_ins (
      'SGM'
    , 'ACL_ACTIONS'
    , 'ID'
    , p_n_column_nk_pos   => 1
   );
   --
   frm_stag_build.prc_build_all ('SGM');
   ROLLBACK;
END;