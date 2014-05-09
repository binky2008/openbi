CREATE PROCEDURE LogPurge(
		 @Month_Count numeric = 12)
AS
BEGIN

	 BEGIN TRAN LogTran;

	 DELETE FROM LogT
	  WHERE
			  LogTimestamp < DATEADD(month, -@Month_Count, CAST(GETDATE()AS date));

	 COMMIT TRAN LogTran;

END;
