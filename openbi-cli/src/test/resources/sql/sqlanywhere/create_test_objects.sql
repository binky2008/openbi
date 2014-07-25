DROP TABLE tab_test;

CREATE TABLE tab_test(
            col_bit varBIT(32767) ,
            col_varBIT varBIT(32767) ,
            col_longvarbit long varbit ,
            --
            col_tinyint TINYINT ,
            col_smallint SMALLINT ,
            col_int INT,
            col_bigint BIGINT,
            --
            col_NUMERIC NUMERIC(127,50),
            col_decimal DECIMAL(127,100),
            --
            col_REAL REAL,
            col_FLOAT FLOAT (53),
            col_DOUBLE DOUBLE,
            --
            col_money money,
            col_smallmoney smallmoney,
            --
            col_char CHAR (32767),
            col_VARCHAR VARCHAR (32767),
            col_LONGVARCHAR LONG VARCHAR,
            col_TEXT TEXT,
            col_nchar NCHAR (32767),
            col_nVARCHAR NVARCHAR (32767),
            col_nLONGVARCHAR LONG NVARCHAR,
            col_NTEXT NTEXT,
            col_UNIQUEIDENTIFIERSTR UNIQUEIDENTIFIERSTR,
            col_XML XML,
            --
            col_binary binary (32767) null,
            col_VARbinary VARbinary (32767) null,
            col_LONGVARbinary LONG binary null,
            col_UNIQUEIDENTIFIER UNIQUEIDENTIFIER null,
            col_image IMAGE null,
            --
            col_date DATE null,
            col_time TIME null,
            col_DATETIME DATETIME,
            col_SMALLDATETIME SMALLDATETIME null,
            col_TIMESTAMP TIMESTAMP,
            col_TIMESTAMPtz TIMESTAMP WITH TIME ZONE,
            col_DATETIMEoffset DATETIMEOFFSET null
        );