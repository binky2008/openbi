CREATE OR REPLACE VIEW etl_stage_curr_hist_v
AS
	SELECT
		sc.etl_stage_source_code
	, ch.etl_stage_curr_object_id
	, cu.etl_stage_object_name AS etl_stage_curr_object_name
	, ch.etl_stage_hist_object_id
	, hi.etl_stage_object_name AS etl_stage_hist_object_name
	FROM
		etl_stage_curr_hist_t ch
	, etl_stage_object_t hi
	, etl_stage_object_t cu
	, etl_stage_source_t sc
	WHERE
		hi.etl_stage_object_id    = ch.etl_stage_hist_object_id
	AND cu.etl_stage_object_id = ch.etl_stage_curr_object_id
	AND cu.etl_stage_source_id = sc.etl_stage_source_id
	ORDER BY
		sc.etl_stage_source_code
	, cu.etl_stage_object_name
	, hi.etl_stage_object_name;

COMMENT ON TABLE etl_stage_curr_hist_v  IS
   '$Author: nmarangoni $
$Date: $
$Revision: $
$Id: $
$HeadURL: $';

GRANT SELECT ON etl_stage_curr_hist_v TO PUBLIC;

BEGIN
	pkg_utl_ddl.prc_create_synonym ('etl_stage_curr_hist_v', 'etl_stage_curr_hist_v', TRUE) ;
END;
/
