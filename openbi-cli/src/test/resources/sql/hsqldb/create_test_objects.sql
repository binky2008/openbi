DROP TABLE test.tab_test;

CREATE TABLE test.tab_test(
            col_boolean BOOLEAN ,
            col_bit BIT(1000000000) ,
            col_bitvarying BIT VARYING(1000000000) ,
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
            col_DOUBLE DOUBLE,
            --
            col_char CHAR (1000),
            col_VARCHAR VARCHAR (1g),
            col_LONGVARCHAR LONGVARCHAR (1000000000),
            col_CLOB CLOB (1000000000g),
            --
            col_binary binary (1000000000000000000),
            col_VARbinary VARbinary (1g),
            col_LONGVARbinary LONGVARbinary (1000000000000000000),
            col_BLOB BLOB (1000000000g),
            --
            col_date DATE,
            col_time TIME,
            col_timetz TIME WITH TIME ZONE,
            col_TIMESTAMP TIMESTAMP,
            col_TIMESTAMPtz TIMESTAMP WITH TIME ZONE,
            --
            col_intervalyd interval year to month,
            col_intervalds interval day to second
        );