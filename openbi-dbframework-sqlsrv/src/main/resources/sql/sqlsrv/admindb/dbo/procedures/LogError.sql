CREATE PROCEDURE LogError(
		 @Text_Short varchar(8000)
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
						  LogTextShort
						, LogObjectID
						, LogObjectName
						, LogDBSessionID
						, LogDBID
						, LogDBName
						, LogDBUser)
		  VALUES(@Text_Short
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
