DROP TABLE tab_test;

CREATE TABLE tab_test (
	col_bit bit,
	col_tinyint tinyint,
	col_smallint smallint,
	col_int int,
	col_bigint bigint,
	col_numeric numeric(20,9),
	col_decimal decimal(30,9),
	col_smallmoney smallmoney,
	col_money money,
	col_real real,
	col_float float,
	--
	col_binary binary(12),
	col_varbinary varbinary,
	col_varbinarymax varbinary(max),
	col_image image,
	col_char char(765),
	col_varchar varchar(4000),
	col_varcharmax varchar(max),
	col_text text,
	col_nchar nchar(1432),
	col_nvarchar nvarchar(4000),
	col_nvarcharmax nvarchar(max),
	col_ntext ntext,
	col_xml xml,
	--
	col_date date,
	col_time time,
	col_datetime datetime,
	col_smalldatetime smalldatetime,
	col_datetime2 datetime2,
	col_datetimeoffset datetimeoffset,
	--
	col_rowversion rowversion,
	col_hierarchyid hierarchyid,
	col_uniqueidentifier uniqueidentifier,
	col_geography geography,
	col_geometry geometry
);