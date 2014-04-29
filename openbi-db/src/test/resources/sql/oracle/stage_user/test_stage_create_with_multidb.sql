BEGIN
   stag_meta.prc_source_ins (
      'SGM'
    , 'SGM'
    , 'SugarCRM Multiple'
    , 'STAGE'
    , 'USERS'
    , 'USERS'
    , 'USERS'
    , 'USERS'
   );
   --
   stag_meta.prc_source_db_ins (
      'SGM'
    , 'SG1'
    , 'SUGARCRM'
    , 'SUGARCRM'
   );
   stag_meta.prc_source_db_ins (
      'SGM'
    , 'SG2'
    , 'SUGARCRM'
    , 'SUGARCRM'
   );
   stag_meta.prc_object_ins (
      'SGM'
    , 'ACCOUNTS'
   );
   stag_meta.prc_object_ins (
      'SGM'
    , 'ACL_ACTIONS'
   );
   --
   dwhadmin.stag_meta.prc_column_import_from_source ('SGM');
   --
   dwhadmin.stag_meta.prc_column_ins (
      'SGM'
    , 'ACCOUNTS'
    , 'DESCRIPTION'
    , p_n_column_edwh_flag   => 0
   );
   --
   dwhadmin.stag_meta.prc_column_ins (
      'SGM'
    , 'ACL_ACTIONS'
    , 'ID'
    , p_n_column_nk_pos   => 1
   );
   --
   dwhadmin.stag_build.prc_build_all ('SGM');
END;