CREATE TABLE dbo.LogT (
				 [LogID] numeric(18)IDENTITY(1, 1)
										 NOT NULL
			  , [LogSource] varchar(80)DEFAULT 'LOG'
											  NULL
			  , [LogTimestamp] datetime DEFAULT GETDATE()
												NULL
			  , [LogSeverity] numeric(18)DEFAULT ERROR_SEVERITY()
												 NULL
			  , [LogAuditActionID] varchar(80)DEFAULT 'LOG'
														  NULL
			  , [LogAuditSequenceNumber] numeric(18)NULL
			  , [LogErrorNumber] numeric(18)DEFAULT ERROR_NUMBER()
													  NULL
			  , [LogErrorState] numeric(18)DEFAULT ERROR_STATE()
													 NULL
			  , [LogRowCount] numeric(18)DEFAULT @@rowcount
												 NULL
			  , [LogTextShort] varchar(8000)NULL
			  , [LogTextLong] varchar(max)DEFAULT ERROR_MESSAGE()
													NULL
			  , [LogObjectID] numeric(18)DEFAULT OBJECT_ID(ERROR_PROCEDURE())
												  NULL
			  , [LogObjectName] varchar(8000)DEFAULT ERROR_PROCEDURE()
														NULL
			  , [LogObjectLine] numeric(18)DEFAULT ERROR_LINE()
													 NULL
			  , [LogDBSessionID] numeric(18)DEFAULT @@spid
														NULL
			  , [LogDBID] numeric(18)DEFAULT DB_ID()
											 NULL
			  , [LogDBName] varchar(8000)DEFAULT DB_NAME()
												  NULL
			  , [LogDBUser] varchar(8000)DEFAULT USER_NAME()
												  NULL
			  , [LogClientOSUser] varchar(8000)DEFAULT SUSER_SNAME()
															NULL
			  , [LogClientHostID] numeric(18)DEFAULT HOST_ID()
														 NULL
			  , [LogClientHostName] varchar(8000)DEFAULT HOST_NAME()
															  NULL
			  , [LogClientAppName] varchar(8000)DEFAULT APP_NAME()
															 NULL);