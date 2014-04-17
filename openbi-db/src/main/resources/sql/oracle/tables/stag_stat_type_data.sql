BEGIN
	stag_meta.prc_stat_type_ins ( 'SIN', 'INSERT STAGE', 'Extract rows from source into stage table') ;
    stag_meta.prc_stat_type_ins ( 'SAN', 'ANALYZE', 'Analyze stage table') ;
	stag_meta.prc_stat_type_ins ( 'DIN', 'INSERT DIFFERENCE', 'Get Source-Target difference into the diff table') ;
    stag_meta.prc_stat_type_ins ( 'DAN', 'ANALYZE', 'Analyze diff table') ;
	stag_meta.prc_stat_type_ins ( 'HUP', 'UPDATE HISTORY', 'Update hist table with rows from the diff table') ;
	stag_meta.prc_stat_type_ins ( 'HIN', 'INSERT HISTORY', 'Insert in hist table with new rows from the diff table') ;
    stag_meta.prc_stat_type_ins ( 'HAN', 'ANALYZE', 'Analyze hist table') ;
END;