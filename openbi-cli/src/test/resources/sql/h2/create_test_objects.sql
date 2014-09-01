DROP TABLE test.tab_test;

CREATE TABLE test.tab_test(
            col_boolean BOOLEAN ,
            --
            --col_IDENTITY IDENTITY ,
            --
            col_tinyint TINYINT ,
            col_smallint SMALLINT ,
            col_bigint BIGINT,
            col_int INT,
            --
            col_NUMERIC NUMERIC(1000000000,5000000),
            col_decimal DECIMAL(1000000000,5000000),
            --
            col_REAL REAL,
            col_FLOAT FLOAT (53),
            col_DOUBLE DOUBLE (53),
            --
            col_char CHAR (255),
            col_VARCHAR VARCHAR (1g),
            col_VARCHARIGNORECASE VARCHAR_IGNORECASE (1g),
            col_CLOB CLOB (1000000000g),
            --
            col_binary binary (255),
            col_VARbinary VARbinary (1g),
            col_BLOB BLOB (1000000000g),
            --
            col_date DATE,
            col_time TIME,
            col_TIMESTAMP TIMESTAMP,
            --
			col_uuid UUID,
			col_OTHER OTHER,
			col_GEOMETRY GEOMETRY,
			col_ARRAY ARRAY
        );