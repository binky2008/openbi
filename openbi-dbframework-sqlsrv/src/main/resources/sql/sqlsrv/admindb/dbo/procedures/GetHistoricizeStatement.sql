CREATE PROCEDURE [dbo].[GetHistoricizeStatement]
		@SourceDatabase varchar(4000)
	 , @SourceSchema varchar(4000)
	 , @SourceObject varchar(4000)
	 , @TargetDatabase varchar(4000)
	 , @TargetSchema varchar(4000)
	 , @TargetObject varchar(4000)
	 , @HistoricizeStatement varchar(max)OUTPUT
AS
BEGIN

	 DECLARE
		 @HistoricizeTemplate varchar(max) = '-- Update and historicize target table with a single statement
MERGE #TargetTable# trg
USING #SourceExpression# src
ON #MergeClause#
AND trg.ValidTo = CAST(''#MaxDate#'' AS date)
WHEN MATCHED
		THEN UPDATE SET
							 #UpdateClause#
WHEN NOT MATCHED BY TARGET
		THEN INSERT(
						#TargetColumnList#
					 , ValidFrom
					 , ValidTo)VALUES(#SourceColumnList#
										  , GETDATE()
										  , CAST(''#MaxDate#'' AS date))
WHEN NOT MATCHED BY SOURCE
		 AND trg.ValidTo = CAST(''#MaxDate#'' AS date)
		THEN UPDATE SET
							 ValidTo = GETDATE();'
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

	 SET @HistoricizeStatement = REPLACE(@HistoricizeTemplate, '#MaxDate#', @MaxDate);
	 SET @HistoricizeStatement = REPLACE(@HistoricizeStatement, '#SourceExpression#', @SourceIdentifier);
	 SET @HistoricizeStatement = REPLACE(@HistoricizeStatement, '#TargetTable#', @TargetIdentifier);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'ALL', 'PREFIX', 'src', @List = @Cols OUTPUT;
	 SET @HistoricizeStatement = REPLACE(@HistoricizeStatement, '#SourceColumnList#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'ALL', 'SIMPLE', @List = @Cols OUTPUT;
	 SET @HistoricizeStatement = REPLACE(@HistoricizeStatement, '#TargetColumnList#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'ALL', 'EQUAL', @List = @Cols OUTPUT;
	 SET @HistoricizeStatement = REPLACE(@HistoricizeStatement, '#MergeClause#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'NPK', 'SET', @List = @Cols OUTPUT;
	 SET @HistoricizeStatement = REPLACE(@HistoricizeStatement, '#UpdateClause#', @Cols);
	 --PRINT @HistoricizeStatement;

END;