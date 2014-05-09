CREATE PROCEDURE [dbo].[GetReconcileStatement]
		@SourceDatabase varchar(4000)
	 , @SourceSchema varchar(4000)
	 , @SourceObject varchar(4000)
	 , @TargetDatabase varchar(4000)
	 , @TargetSchema varchar(4000)
	 , @TargetObject varchar(4000)
	 , @ReconcileStatement varchar(max)OUTPUT
AS
BEGIN

	 DECLARE
		 @ReconcileTemplate varchar(max) = '-- Reconcile changes previously detected in a change table in the target table
DECLARE @Now datetime = GETDATE();
-- Close validity of deleted rows and previous history of changed rows
MERGE #TargetTable# trg
USING (SELECT * FROM #ChangeTable# WHERE DMLOperation IN (''D'',''H'')) src
ON #MergeClause#
AND trg.ValidTo = CAST(''#MaxDate#'' AS date)
WHEN MATCHED
	 THEN UPDATE SET
		  DMLOperation = src.DMLOperation
		, ValidTo = CAST(''#MaxDate#'' AS date);

-- Physical updates (no history)
MERGE #TargetTable# trg
USING (SELECT * FROM #ChangeTable# WHERE DMLOperation IN (''U'')) src
ON #MergeClause#
AND trg.ValidTo = CAST(''#MaxDate#'' AS date)
WHEN MATCHED
	 THEN UPDATE SET
		  #UpdateClause#
		, ValidFrom = @Now;

-- Insert new rows
INSERT INTO #TargetTable# (
	 #ColumnList#
  , ValidFrom
  , ValidTo
)
SELECT #ColumnList#
	  , @Now
	  , CAST(''#MaxDate#'' AS date)
  FROM #ChangeTable#
 WHERE DMLOperation = IN (''H'',''I'');'
	  , @MaxDate varchar(10) = '9999-12-31'
	  , @Cols varchar(max)
	  , @SourceIdentifier varchar(4000)
	  , @TargetIdentifier varchar(4000);

	 -- Set database and schema names
	 SET @SourceDatabase = ISNULL(@SourceDatabase, DB_NAME());
	 SET @SourceSchema = ISNULL(@SourceSchema, SCHEMA_NAME());
	 SET @TargetDatabase = ISNULL(@TargetDatabase, DB_NAME());
	 SET @TargetSchema = ISNULL(@TargetSchema, SCHEMA_NAME());

	 -- Set source and target identifier
	 SET @SourceIdentifier = CASE
										 WHEN @SourceDatabase != DB_NAME()
											 THEN @SourceDatabase + '.'
										 ELSE ''
									 END + @SourceSchema + '.' + @SourceObject;
	 SET @TargetIdentifier = CASE
										 WHEN @TargetDatabase != DB_NAME()
											 THEN @TargetDatabase + '.'
										 ELSE ''
									 END + @TargetSchema + '.' + @TargetObject;

	 SET @ReconcileStatement = REPLACE(@ReconcileTemplate, '#MaxDate#', @MaxDate);
	 SET @ReconcileStatement = REPLACE(@ReconcileStatement, '#ChangeTable#', @SourceIdentifier);
	 SET @ReconcileStatement = REPLACE(@ReconcileStatement, '#TargetTable#', @TargetIdentifier);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'PK', 'EQUAL', @List = @Cols OUTPUT;
	 SET @ReconcileStatement = REPLACE(@ReconcileStatement, '#MergeClause#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'NPK', 'SET', @List = @Cols OUTPUT;
	 SET @ReconcileStatement = REPLACE(@ReconcileStatement, '#UpdateClause#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'ALL', 'SIMPLE', @List = @Cols OUTPUT;
	 SET @ReconcileStatement = REPLACE(@ReconcileStatement, '#ColumnList#', @Cols);
	 --
	 --PRINT @ReconcileStatement;

END;