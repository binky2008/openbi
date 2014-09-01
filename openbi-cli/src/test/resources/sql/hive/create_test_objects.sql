DROP TABLE tab_test;

CREATE TABLE tab_test (
	COL_BOOLEAN BOOLEAN,
	--
	COL_TINYINT TINYINT,
	COL_SMALLINT SMALLINT,
	COL_INT INT,
	COL_BIGINT BIGINT,
	--
	COL_DECIMAL DECIMAL,
	--
	COL_DOUBLE DOUBLE,
	COL_FLOAT FLOAT,
	--
	COL_TIMESTAMP TIMESTAMP,
	COL_DATE DATE,
	--
	COL_CHAR CHAR(255),
	COL_VARCHAR VARCHAR(65355),
	COL_STRING string,
	--
	COL_BINARY BINARY
)  ROW FORMAT delimited fields terminated by ',';

grant select on tab_test to user hue;