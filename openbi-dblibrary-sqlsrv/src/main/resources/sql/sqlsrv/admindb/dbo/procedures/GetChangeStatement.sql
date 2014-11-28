CREATE PROCEDURE [dbo].[GetChangeStatement]
		@SourceDatabase varchar(4000)
	 , @SourceSchema varchar(4000)
	 , @SourceObject varchar(4000)
	 , @TargetDatabase varchar(4000)
	 , @TargetSchema varchar(4000)
	 , @TargetObject varchar(4000)
	 , @ChangeDatabase varchar(4000)
	 , @ChangeSchema varchar(4000)
	 , @ChangeObject varchar(4000)
	 , @ChangeStatement varchar(max)OUTPUT
AS
BEGIN

	 DECLARE
		 @ChangeTemplate varchar(max) = '-- Detect changes
TRUNCATE TABLE #changeTable#
INSERT INTO #changeTable#
	 (DMLOperation, #insertColumnList#)
SELECT DMLOperation, #insertColumnList#
  FROM(
		 SELECT
				  CASE
					  WHEN #insertClause#
						  THEN ''I''
					  WHEN #deleteClause#
						  THEN ''D''
					  WHEN #updateClause#
						  THEN ''U''
					  WHEN #historicizeClause#
						  THEN ''H''
				  END AS DMLOperation
				, #PKcolumnList#
				, #NPKcolumnList#
			FROM #sourceExpression# AS src
				  FULL OUTER JOIN
		(SELECT *
			FROM #targetTable#
		  WHERE ValidTo = ''#MaxDate#'') AS trg
			  ON #joinClause#)AS t
 WHERE DMLOperation IS NOT NULL;'
	  , @MaxDate varchar(10) = '9999-12-31'
	  , @Cols varchar(max)
	  , @SourceIdentifier varchar(4000)
	  , @TargetIdentifier varchar(4000)
	  , @ChangeIdentifier varchar(4000);

	 -- Set database and schema names
	 SET @SourceDatabase = ISNULL(@SourceDatabase, DB_NAME());
	 SET @SourceSchema = ISNULL(@SourceSchema, SCHEMA_NAME());
	 SET @TargetDatabase = ISNULL(@TargetDatabase, DB_NAME());
	 SET @TargetSchema = ISNULL(@TargetSchema, SCHEMA_NAME());
	 SET @ChangeDatabase = ISNULL(@ChangeDatabase, DB_NAME());
	 SET @ChangeSchema = ISNULL(@ChangeSchema, SCHEMA_NAME());

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
	 SET @ChangeIdentifier = CASE
										 WHEN @ChangeDatabase != DB_NAME()
											 THEN @ChangeDatabase + '.'
										 ELSE ''
									 END + @ChangeSchema + '.' + @ChangeObject;

	 SET @ChangeStatement = REPLACE(@ChangeTemplate, '#MaxDate#', @MaxDate);
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#SourceExpression#', @SourceIdentifier);
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#TargetTable#', @TargetIdentifier);
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#ChangeTable#', @ChangeIdentifier);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'ALL', 'SIMPLE', @List = @Cols OUTPUT;
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#insertColumnList#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'PK', 'EQUAL', @List = @Cols OUTPUT;
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#JoinClause#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'PK', 'IS_NULL', 'src', @List = @Cols OUTPUT;
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#deleteClause#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'PK', 'IS_NULL', 'trg', @List = @Cols OUTPUT;
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#insertClause#', @Cols);
	 --
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#UpdateClause#', '1=0');
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'NPK', 'NOT_EQUAL', @List = @Cols OUTPUT;
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#HistoricizeClause#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'PK', 'ISNULL', 'src', 'trg', @List = @Cols OUTPUT;
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#PKcolumnList#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'NPK', 'CHANGE', @List = @Cols OUTPUT;
	 SET @ChangeStatement = REPLACE(@ChangeStatement, '#NPKcolumnList#', @Cols);
	 --
	 --PRINT @ChangeStatement;

END;