CREATE OR REPLACE VIEW etl_stage_size_v
AS
	SELECT
		sc.etl_stage_source_code
	, ob.etl_stage_object_id
	, ob.etl_stage_object_name
	, si.etl_stage_table_name
	, si.etl_stage_num_rows
	, si.etl_stage_bytes
	, si.create_date
	FROM
		etl_stage_size_t si
	, etl_stage_object_t ob
	, etl_stage_source_t sc
	WHERE
		si.etl_stage_object_id = ob.etl_stage_object_id
	AND ob.etl_stage_source_id = sc.etl_stage_source_id
	ORDER BY
		si.create_date DESC;

COMMENT ON TABLE etl_stage_size_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON etl_stage_size_v TO PUBLIC;

BEGIN
	pkg_utl_ddl.prc_create_synonym ('etl_stage_size_v', 'etl_stage_size_v', TRUE) ;
END;
/
