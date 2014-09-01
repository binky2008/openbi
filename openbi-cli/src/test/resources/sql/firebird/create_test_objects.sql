DROP TABLE tab_test;

CREATE TABLE tab_test(
            col_smallint SMALLINT ,
            col_bigint BIGINT,
            col_int INT,
            --
            col_NUMERIC NUMERIC(18,10),
            col_decimal DECIMAL(18,10),
            --
            col_FLOAT FLOAT  (4000),
            col_DOUBLE DOUBLE precision,
            --
            col_char CHAR (255),
            col_VARCHAR VARCHAR (4000),
            --
            col_date DATE,
            col_time TIME,
            col_TIMESTAMP TIMESTAMP,
            --
            col_BLOB BLOB
        );