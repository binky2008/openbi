BEGIN
	stag_meta.prc_stat_type_ins ( 'ANL', 'ANALYZE', 'Analyze table') ;
	stag_meta.prc_stat_type_ins ( 'INS', 'INSERT', 'Insert rows') ;
	stag_meta.prc_stat_type_ins ( 'IDT', 'INSERT DELTA', 'Insert delta rows in tables without NK') ;
	stag_meta.prc_stat_type_ins ( 'MDT', 'MERGE DELTA', 'Merge delta rows in tables with NK') ;
	stag_meta.prc_stat_type_ins ( 'MDE', 'MERGE DELETED', 'Set operation = D in tables with NK') ;
	stag_meta.prc_stat_type_ins ( 'FDI', 'GET DIFFERENCE', 'Get STG2-STG1 Difference with Full Outer Join') ;
	stag_meta.prc_stat_type_ins ( 'FUP', 'UPDATE DIFFERENCE', 'Update STG2 Table with updated and deleted rows from the Difference table') ;
	stag_meta.prc_stat_type_ins ( 'FIN', 'INSERT DIFFERENCE', 'Insert in STG2 Table with new rows from the Difference table') ;
	stag_meta.prc_stat_type_ins ( 'MHI', 'MERGE HISTORY', 'Merge in STG2 Table with new rows from the History table') ;
END;
/