CREATE PROCEDURE dbo.AuditEvents
	 WITH EXECUTE AS OWNER
AS
BEGIN

	 -- Merge events from the audit files into the audit table

	 WITH cte
		  AS (SELECT
						 CAST(event_time AS datetime) + GETDATE() - GETUTCDATE()AS event_time
					  , sequence_number
					  , action_id
					  , succeeded
					  , permission_bitmask
					  , is_column_permission
					  , session_id
					  , server_principal_id
					  , database_principal_id
					  , target_server_principal_id
					  , target_database_principal_id
					  , object_id
					  , class_type
					  , session_server_principal_name
					  , server_principal_name
					  , server_principal_sid
					  , database_principal_name
					  , target_server_principal_name
					  , target_server_principal_sid
					  , target_database_principal_name
					  , server_instance_name
					  , database_name
					  , schema_name
					  , object_name
					  , statement
					  , additional_information
					  , file_name
					  , audit_file_offset
				  FROM sys.fn_get_audit_file('G:\AuditLogs\*', DEFAULT, DEFAULT))
		  MERGE INTO AuditEventsT AS trg
		  USING(SELECT
							*
					 FROM cte)AS src
		  ON trg.event_time = src.event_time
		 AND trg.sequence_number = src.sequence_number
		 AND trg.object_id = src.object_id
		 AND trg.action_id = src.action_id
		  WHEN NOT MATCHED BY TARGET
				  THEN INSERT(
								  event_time
								, sequence_number
								, action_id
								, succeeded
								, permission_bitmask
								, is_column_permission
								, session_id
								, server_principal_id
								, database_principal_id
								, target_server_principal_id
								, target_database_principal_id
								, object_id
								, class_type
								, session_server_principal_name
								, server_principal_name
								, server_principal_sid
								, database_principal_name
								, target_server_principal_name
								, target_server_principal_sid
								, target_database_principal_name
								, server_instance_name
								, database_name
								, schema_name
								, object_name
								, statement
								, additional_information
								, file_name
								, audit_file_offset)VALUES(event_time
																 , sequence_number
																 , action_id
																 , succeeded
																 , permission_bitmask
																 , is_column_permission
																 , session_id
																 , server_principal_id
																 , database_principal_id
																 , target_server_principal_id
																 , target_database_principal_id
																 , object_id
																 , class_type
																 , session_server_principal_name
																 , server_principal_name
																 , server_principal_sid
																 , database_principal_name
																 , target_server_principal_name
																 , target_server_principal_sid
																 , target_database_principal_name
																 , server_instance_name
																 , database_name
																 , schema_name
																 , object_name
																 , statement
																 , additional_information
																 , file_name
																 , audit_file_offset);

END;
