BEGIN
   ROLLBACK;
   p#frm#stag_meta.prc_source_ins (
      'SGC'
    , 'SGC'
    , 'SugarCRM'
    , 'DWHSTAGE'
    , 'USERS'
    , 'USERS'
    , 'USERS'
    , 'USERS'
   );
   --
   p#frm#stag_meta.prc_source_db_ins (
      'SGC'
    , 'NONE'
    , 'SUGARCRM'
    , 'SUGARCRM'
   );
   p#frm#stag_meta.prc_object_ins (
      'SGC'
    , 'ACCOUNTS'
   );
   p#frm#stag_meta.prc_object_ins (
      'SGC'
    , 'ACL_ACTIONS'
   );
   p#frm#stag_meta.prc_object_ins (
      'SGC'
    , 'CONTACTS'
   );
   p#frm#stag_meta.prc_object_ins (
      'SGC'
    , 'EMAIL_ADDRESSES'
    , p_vc_increment_buffer   => 10
   );
   --
   p#frm#stag_meta.prc_column_import_from_source ('SGC');
   --
   p#frm#stag_meta.prc_column_ins (
      'SGC'
    , 'ACCOUNTS'
    , 'DESCRIPTION'
    , p_n_column_edwh_flag   => 0
   );
   p#frm#stag_meta.prc_column_ins (
      'SGC'
    , 'CONTACTS'
    , 'DESCRIPTION'
    , p_n_column_edwh_flag   => 0
   );
   --
   p#frm#stag_meta.prc_column_ins (
      'SGC'
    , 'ACL_ACTIONS'
    , 'ID'
    , p_n_column_nk_pos   => 1
   );
   --
   p#frm#stag_meta.prc_column_ins (
      'SGC'
    , 'EMAIL_ADDRESSES'
    , 'ID'
    , p_n_column_nk_pos   => 1
   );
   --
   p#frm#stag_meta.prc_column_ins (
      'SGC'
    , 'EMAIL_ADDRESSES'
    , 'DATE_MODIFIED'
    , p_n_column_incr_flag   => 1
   );
   --
   p#frm#stag_build.prc_build_all ('SGC');
   ROLLBACK;
END;