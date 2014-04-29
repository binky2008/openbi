BEGIN
   stag_meta.prc_source_ins (
      'TST'
    , 'TST'
    , 'Test'
    , 'STAGE'
    , 'USERS'
    , 'USERS'
    , 'USERS'
    , 'USERS'
   );
   --
   stag_meta.prc_source_db_ins (
      'TST'
    , 'NONE'
    , 'TEST'
    , 'TEST'
   );
   stag_meta.prc_object_ins (
      'TST'
    , 'BIGTABLE'
    , p_vc_partition_clause   => 'substr( 12345678,-1)'
   );
   --
   --
   dwhadmin.stag_meta.prc_column_import_from_source ('TST');
   --
   dwhadmin.stag_build.prc_build_all ('TST');
END;