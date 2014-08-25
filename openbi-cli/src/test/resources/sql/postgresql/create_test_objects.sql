DROP TABLE if exists test.tab_test;

CREATE TABLE test.tab_test (
	col_bool	bool
,	col_bit	bit(10)
,	col_varbit	varbit(10)
		
,	col_snallserial	smallserial
,	col_serial	serial
,	col_bigserial	bigserial
		
,	col_smallint	smallint
,	col_integer	integer
,	col_bigint	bigint
		
,	col_numeric	numeric (37,5)
,	col_real	real
,	col_double_precision	double precision
,	col_money	money
		
,	col_date	date
,	col_time	time
,	cool_timetz	timetz
,	col_timestamp	timestamp
,	col_timestamptz	timestamptz
		
,	col_interval	interval
		
,	col_char	char (2000)
,	col_varchar	varchar (20000)
,	col_text	text
		
,	col_txid_snapshot	txid_snapshot
,	col_uuid	uuid
,	col_xml	xml
,	col_json	json
		
,	col_bytea	bytea
		
,	col_cidr	cidr
,	col_inet	inet
,	col_macaddr	macaddr
		
,	col_tsquery	tsquery
,	col_tsvector	tsvector
		
		
,	col_box	box
,	col_circle	circle
,	col_line	line
,	col_lseg	lseg
,	col_path	path
,	col_point	point
,	col_polygon	polygon
);

