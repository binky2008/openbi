CREATE TABLE ETL_STAGE_COLUMN_T (ETL_STAGE_COLUMN_ID BIGINT AUTO_INCREMENT NOT NULL, ETL_STAGE_COLUMN_COMMENT VARCHAR(4000), ETL_STAGE_COLUMN_DEF VARCHAR(100), ETL_STAGE_COLUMN_DEF_SRC VARCHAR(100), ETL_STAGE_COLUMN_EDWH_FLAG DECIMAL(22), ETL_STAGE_COLUMN_INCR_FLAG DECIMAL(22), ETL_STAGE_COLUMN_NAME VARCHAR(100), ETL_STAGE_COLUMN_NAME_MAP VARCHAR(100), ETL_STAGE_COLUMN_NK_POS DECIMAL(22), ETL_STAGE_COLUMN_POS DECIMAL(22), ETL_STAGE_OBJECT_ID DECIMAL(22), PRIMARY KEY (ETL_STAGE_COLUMN_ID))
CREATE TABLE ETL_STAGE_COLUMN_CHECK_T (ETL_STAGE_COLUMN_CHECK_ID BIGINT AUTO_INCREMENT NOT NULL, ETL_STAGE_COLUMN_DEF VARCHAR(100), ETL_STAGE_COLUMN_NAME VARCHAR(100), ETL_STAGE_COLUMN_NK_POS DECIMAL(22), ETL_STAGE_COLUMN_POS DECIMAL(22), ETL_STAGE_OBJECT_ID DECIMAL(22), PRIMARY KEY (ETL_STAGE_COLUMN_CHECK_ID))
CREATE TABLE ETL_STAGE_OBJECT_T (ETL_STAGE_OBJECT_ID BIGINT AUTO_INCREMENT NOT NULL, ETL_STAGE_DELTA_FLAG DECIMAL(22), ETL_STAGE_DIFF_NK_NAME VARCHAR(100), ETL_STAGE_DIFF_TABLE_NAME VARCHAR(100), ETL_STAGE_DUPL_TABLE_NAME VARCHAR(100), ETL_STAGE_FILTER_CLAUSE VARCHAR(4000), ETL_STAGE_INCREMENT_BUFFER DECIMAL(22), ETL_STAGE_OBJECT_COMMENT VARCHAR(4000), ETL_STAGE_OBJECT_NAME VARCHAR(100), ETL_STAGE_OBJECT_ROOT VARCHAR(100), ETL_STAGE_PACKAGE_NAME VARCHAR(100), ETL_STAGE_PARALLEL_DEGREE DECIMAL(22), ETL_STAGE_PARTITION_CLAUSE VARCHAR(4000), ETL_STAGE_SOURCE_ID BIGINT NOT NULL, ETL_STAGE_SOURCE_NK_FLAG DECIMAL(22), ETL_STAGE_SRC_TABLE_NAME VARCHAR(100), ETL_STAGE_STG1_TABLE_NAME VARCHAR(100), ETL_STAGE_STG2_NK_NAME VARCHAR(100), ETL_STAGE_STG2_TABLE_NAME VARCHAR(100), ETL_STAGE_STG2_VIEW_NAME VARCHAR(100), PRIMARY KEY (ETL_STAGE_OBJECT_ID))
CREATE TABLE ETL_STAGE_SOURCE_T (ETL_STAGE_SOURCE_ID BIGINT AUTO_INCREMENT NOT NULL, ETL_STAGE_BODI_DS VARCHAR(100), ETL_STAGE_OWNER VARCHAR(100), ETL_STAGE_SOURCE_BODI_DS VARCHAR(100), ETL_STAGE_SOURCE_CODE VARCHAR(10), ETL_STAGE_SOURCE_NAME VARCHAR(1000), ETL_STAGE_SOURCE_PREFIX VARCHAR(10), ETL_STAGE_TS_STG1_DATA VARCHAR(100), ETL_STAGE_TS_STG1_INDX VARCHAR(100), ETL_STAGE_TS_STG2_DATA VARCHAR(100), ETL_STAGE_TS_STG2_INDX VARCHAR(100), PRIMARY KEY (ETL_STAGE_SOURCE_ID))
CREATE TABLE ETL_STAGE_SOURCE_DB_T (ETL_STAGE_SOURCE_DB_ID BIGINT AUTO_INCREMENT NOT NULL, ETL_STAGE_DISTRIBUTION_CODE VARCHAR(10), ETL_STAGE_SOURCE_BODI_DS VARCHAR(100), ETL_STAGE_SOURCE_DB_JDBCNAME VARCHAR(100), ETL_STAGE_SOURCE_DB_LINK VARCHAR(100), ETL_STAGE_SOURCE_ID BIGINT, ETL_STAGE_SOURCE_OWNER VARCHAR(100), PRIMARY KEY (ETL_STAGE_SOURCE_DB_ID))
ALTER TABLE ETL_STAGE_OBJECT_T ADD CONSTRAINT FK_ETL_STAGE_OBJECT_T_ETL_STAGE_SOURCE_ID FOREIGN KEY (ETL_STAGE_SOURCE_ID) REFERENCES ETL_STAGE_SOURCE_T (ETL_STAGE_SOURCE_ID)
ALTER TABLE ETL_STAGE_SOURCE_DB_T ADD CONSTRAINT FK_ETL_STAGE_SOURCE_DB_T_ETL_STAGE_SOURCE_ID FOREIGN KEY (ETL_STAGE_SOURCE_ID) REFERENCES ETL_STAGE_SOURCE_T (ETL_STAGE_SOURCE_ID)
