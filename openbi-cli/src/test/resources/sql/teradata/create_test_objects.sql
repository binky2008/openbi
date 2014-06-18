DROP TABLE tab_test;

CREATE TABLE tab_test (
	COL_Byte Byte (1000),
	COL_VARByte VARByte (1000),
	COL_BLOB BLOB(1g),
	--
	COL_CHAR Char(1000) char set unicode,
	COL_VARCHAR Varchar(1000) char set latin,
	COL_CLOB CLOB(1g),
	--
	COL_DATE Date,
	COL_TIME Time,
	COL_TIMETZ Time with time zone,
	COL_TIMESTAMP Timestamp,
	COL_TIMESTAMPTZ Timestamp with time zone,
	--
	COL_periodD PERIOD(DATE),
	COL_periodT PERIOD(TIME),
	COL_periodTZ PERIOD(Time with time zone),
	COL_periodts PERIOD(Timestamp),
	COL_periodtsTZ PERIOD(Timestamp with time zone),
	--
	COL_INTERVALDS Interval Day to Second,
	COL_INTERVALym Interval year to month,
	--
	COL_DOUBLE DOUBLE PRECISION,
	COL_FLOAT FLOAT (54),
	COL_REAL REAL,
	--
	COL_INTEGER INTEGER,
	COL_INT INT,
	COL_BIGINT BIGINT,
	COL_SMALLINT SMALLINT,
	COL_BYTEINT BYTEINT,
	COL_DECIMAL DECIMAL(38,20),
	COL_NUMERIC NUMERIC(38,20),
	COL_NUMBER NUMBER(38,20)
);