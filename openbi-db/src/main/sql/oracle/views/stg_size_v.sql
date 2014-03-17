CREATE OR REPLACE VIEW stg_size_v
AS
	SELECT
		sc.stg_source_code
	, ob.stg_object_id
	, ob.stg_object_name
	, si.stg_table_name
	, si.stg_num_rows
	, si.stg_bytes
	, si.create_date
	FROM
		stg_size_t si
	, stg_object_t ob
	, stg_source_t sc
	WHERE
		si.stg_object_id = ob.stg_object_id
	AND ob.stg_source_id = sc.stg_source_id
	ORDER BY
		si.create_date DESC;

COMMENT ON TABLE stg_size_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stg_size_v TO PUBLIC;

BEGIN
	ddl.prc_create_synonym ('stg_size_v', 'stg_size_v', TRUE) ;
END;
/
