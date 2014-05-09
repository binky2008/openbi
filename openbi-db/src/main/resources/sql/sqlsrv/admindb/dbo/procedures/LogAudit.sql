CREATE PROCEDURE LogAudit
AS
BEGIN
	 MERGE INTO LogT AS trg
	 USING(SELECT
					  *
				FROM AuditEventsT)AS src
	 ON trg.LogSource = 'AUDIT'
	AND trg.LogTimestamp = src.event_time
	AND trg.LogAuditSequenceNumber = src.sequence_number
	AND trg.LogObjectID = src.object_id
	AND trg.LogAuditActionID = src.action_id
	 WHEN NOT MATCHED BY TARGET
			 THEN INSERT(
							 LogTimestamp
						  , LogSource
						  , LogAuditActionID
						  , LogAuditSequenceNumber
						  , LogSeverity
						  , LogTextShort
						  , LogTextLong
						  , LogObjectID
						  , LogObjectName
						  , LogDBSessionID
						  , LogDBID
						  , LogDBName
						  , LogDBUser
						  , LogClientOSUser
						  , LogClientHostID
						  , LogClientHostName
						  , LogClientAppName)VALUES(src.event_time
															  , 'AUDIT'
															  , src.action_id
															  , src.sequence_number
															  , CASE
																	 WHEN src.succeeded = 1
																		 THEN 10
																	 ELSE 11
																 END
															  , NULL
															  , src.statement
															  , src.object_id
															  , src.object_name
															  , src.session_id
															  , DB_ID(src.database_name)
															  , src.database_name
															  , src.database_principal_name
															  , NULL
															  , NULL
															  , NULL
															  , NULL);
END;
