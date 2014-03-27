CREATE OR REPLACE VIEW stag_size_v
AS
	SELECT
		sc.stag_source_code
	, ob.stag_object_id
	, ob.stag_object_name
	, si.stag_table_name
	, si.stag_num_rows
	, si.stag_bytes
	, si.create_date
	FROM
		stag_size_t si
	, stag_object_t ob
	, stag_source_t sc
	WHERE
		si.stag_object_id = ob.stag_object_id
	AND ob.stag_source_id = sc.stag_source_id
	ORDER BY
		si.create_date DESC;

COMMENT ON TABLE stag_size_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON stag_size_v TO PUBLIC;
