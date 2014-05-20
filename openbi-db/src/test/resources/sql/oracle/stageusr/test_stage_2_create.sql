BEGIN
   ROLLBACK;
   frm_stag_meta.prc_source_ins (
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
   frm_stag_meta.prc_source_db_ins (
      'SGC'
    , 'NONE'
    , 'SUGARCRM'
    , 'SUGARCRM'
   );
   frm_stag_meta.prc_object_ins (
      'SGC'
    , 'ACCOUNTS'
   );
   frm_stag_meta.prc_object_ins (
      'SGC'
    , 'ACL_ACTIONS'
   );
   frm_stag_meta.prc_object_ins (
      'SGC'
    , 'CONTACTS'
   );
   frm_stag_meta.prc_object_ins (
      'SGC'
    , 'EMAIL_ADDRESSES'
    , p_vc_increment_buffer   => 10
   );
   --
   frm_stag_meta.prc_column_import_from_source ('SGC');
   --
   frm_stag_meta.prc_column_ins (
      'SGC'
    , 'ACCOUNTS'
    , 'DESCRIPTION'
    , p_n_column_edwh_flag   => 0
   );
   frm_stag_meta.prc_column_ins (
      'SGC'
    , 'CONTACTS'
    , 'DESCRIPTION'
    , p_n_column_edwh_flag   => 0
   );
   --
   frm_stag_meta.prc_column_ins (
      'SGC'
    , 'ACL_ACTIONS'
    , 'ID'
    , p_n_column_nk_pos   => 1
   );
   --
   frm_stag_meta.prc_column_ins (
      'SGC'
    , 'EMAIL_ADDRESSES'
    , 'ID'
    , p_n_column_nk_pos   => 1
   );
   --
   frm_stag_meta.prc_column_ins (
      'SGC'
    , 'EMAIL_ADDRESSES'
    , 'DATE_MODIFIED'
    , p_n_column_incr_flag   => 1
   );
   --
   frm_stag_build.prc_build_all ('SGC');
   ROLLBACK;
END;