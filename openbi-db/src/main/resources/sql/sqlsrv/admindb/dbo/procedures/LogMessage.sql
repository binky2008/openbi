CREATE PROCEDURE LogMessage(
		 @Severity numeric
	  , @Text_Short varchar(8000)
	  , @Text_Long varchar(8000) = NULL
	  , @Object_ID numeric = NULL
	  , @Object_Name varchar(8000) = NULL
	  , @DB_Session_ID numeric = @@SPID
	  , @DB_ID numeric = NULL
	  , @DB_Name varchar(8000) = NULL
	  , @DB_User varchar(8000) = NULL)
AS
BEGIN

	 BEGIN TRAN LogTran;

	 BEGIN TRY

		  INSERT INTO LogT(
						  LogSeverity
						, LogTextShort
						, LogTextLong
						, LogObjectID
						, LogObjectName
						, LogDBSessionID
						, LogDBID
						, LogDBName
						, LogDBUser)
		  VALUES(@Severity
				 , @Text_Short
				 , @Text_Long
				 , ISNULL(@Object_ID, OBJECT_ID(ERROR_PROCEDURE()))
				 , ISNULL(@Object_Name, ERROR_PROCEDURE())
				 , @DB_Session_ID
				 , @DB_ID
				 , @DB_Name
				 , @DB_User);
	 END TRY
	 BEGIN CATCH
	 END CATCH;

	 COMMIT TRAN LogTran;

END;
