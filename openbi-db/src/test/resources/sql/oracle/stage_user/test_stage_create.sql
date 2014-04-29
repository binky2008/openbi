BEGIN
   stag_meta.prc_source_ins (
      'SGC'
    , 'SGC'
    , 'SugarCRM'
    , 'STAGE'
    , 'USERS'
    , 'USERS'
    , 'USERS'
    , 'USERS'
   );
   --
   stag_meta.prc_source_db_ins (
      'SGC'
    , 'NONE'
    , 'SUGARCRM'
    , 'SUGARCRM'
   );
   stag_meta.prc_object_ins (
      'SGC'
    , 'ACCOUNTS'
   );
   stag_meta.prc_object_ins (
      'SGC'
    , 'ACL_ACTIONS'
   );
   stag_meta.prc_object_ins (
      'SGC'
    , 'CONTACTS'
   );
   --
   dwhadmin.stag_meta.prc_column_import_from_source ('SGC');
   --
   dwhadmin.stag_meta.prc_column_ins (
      'SGC'
    , 'ACCOUNTS'
    , 'DESCRIPTION'
    , p_n_column_edwh_flag   => 0
   );
   dwhadmin.stag_meta.prc_column_ins (
      'SGC'
    , 'CONTACTS'
    , 'DESCRIPTION'
    , p_n_column_edwh_flag   => 0
   );
   --
   dwhadmin.stag_meta.prc_column_ins (
      'SGC'
    , 'ACL_ACTIONS'
    , 'ID'
    , p_n_column_nk_pos   => 1
   );
   --
   dwhadmin.stag_build.prc_build_all ('SGC');
END;