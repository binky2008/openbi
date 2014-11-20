BEGIN
	p#frm#stag_meta.prc_stat_type_ins ( 'STIN', 'INSERT STAGE', 'Extract rows from source into stage table') ;
    p#frm#stag_meta.prc_stat_type_ins ( 'STAN', 'ANALYZE', 'Analyze stage table') ;
    p#frm#stag_meta.prc_stat_type_ins ( 'DUIN', 'INSERT DIFFERENCE', 'Get Source-Target difference into the diff table') ;
    p#frm#stag_meta.prc_stat_type_ins ( 'DUAN', 'ANALYZE', 'Analyze diff table') ;
    p#frm#stag_meta.prc_stat_type_ins ( 'DFIN', 'INSERT DIFFERENCE', 'Get Source-Target difference into the diff table') ;
    p#frm#stag_meta.prc_stat_type_ins ( 'DFAN', 'ANALYZE', 'Analyze diff table') ;
    p#frm#stag_meta.prc_stat_type_ins ( 'HSCL', 'CLOSE HISTORY', 'Close validity of old and deleted records') ;
    p#frm#stag_meta.prc_stat_type_ins ( 'HSUP', 'UPDATE HISTORY', 'Update hist table with rows from the diff table') ;
	p#frm#stag_meta.prc_stat_type_ins ( 'HSIN', 'INSERT HISTORY', 'Insert in hist table with new rows from the diff table') ;
    p#frm#stag_meta.prc_stat_type_ins ( 'HSAN', 'ANALYZE', 'Analyze hist table') ;
END;