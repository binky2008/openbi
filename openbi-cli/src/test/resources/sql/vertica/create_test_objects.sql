DROP TABLE tab_test;

CREATE TABLE tab_test (
	COL_BINARY Binary (65000),
	COL_VARBINARY Varbinary (65000),
	COL_LONGVARBINARY long Varbinary (32000000),
	COL_BYTEA Varbinary (65000),
	COL_RAW Varbinary (65000),
	--
	COL_BOOLEAN Boolean,
	--
	COL_CHAR Char(30000),
	COL_VARCHAR Varchar(30000),
	COL_LONGVARCHAR long Varchar(32000000),
	--
	COL_DATE Date,
	COL_DATETIME DATETIME,
	COL_SMALLDATETIME SMALLDATETIME,
	COL_TIME Time,
	COL_TIMETZ TimeTz,
	COL_TIMESTAMP Timestamp,
	COL_TIMESTAMPTZ TimestampTz,
	--
	COL_INTERVALDS Interval Day to Second,
	COL_INTERVALym Interval year to month,
	--
	COL_DOUBLE Float,
	COL_FLOAT Float,
	COL_FLOATN Float (),
	COL_FLOAT8 Float,
	COL_REAL Float,
	--
	COL_INTEGER Integer,
	COL_INT Integer,
	COL_BIGINT Integer,
	COL_INT8 Integer,
	COL_SMALLINT Integer,
	COL_TINYINT Integer,
	COL_DECIMAL Numeric,
	COL_NUMERIC Numeric,
	COL_NUMBER Numeric,
	COL_MONEY Numeric
);