CREATE PROCEDURE dbo.GetTableStatement
		@SourceDatabase varchar(4000)
	 , @SourceSchema varchar(4000)
	 , @SourceObject varchar(4000)
	 , @TargetDatabase varchar(4000)
	 , @TargetSchema varchar(4000)
	 , @TargetObject varchar(4000)
	 , @AdditionalColumns varchar(4000)
	 , @TableStatement varchar(max)OUTPUT
AS
BEGIN

	 DECLARE
		 @ChangeTemplate varchar(max) = 'CREATE TABLE #TableName# (#CoulumnDefinition# #AdditionalColumns# #PrimaryKey#)'
	  , @MaxDate varchar(10) = '9999-12-31'
	  , @Cols varchar(max)
	  , @SourceIdentifier varchar(4000)
	  , @TargetIdentifier varchar(4000)
	  , @PKDefinition varchar(4000) = '';

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

	 SET @TableStatement = REPLACE(@ChangeTemplate, '#TableName#', @TargetIdentifier);
	 SET @TableStatement = REPLACE(@TableStatement, '#AdditionalColumns#', @AdditionalColumns);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'ALL', 'DEFINITION', @List = @Cols OUTPUT;
	 SET @TableStatement = REPLACE(@TableStatement, '#CoulumnDefinition#', @Cols);
	 --
	 EXEC dbo.GetListOfColumns @SourceDatabase, @SourceSchema, @SourceObject, 'PK', 'SIMPLE', @List = @Cols OUTPUT;
	 IF @Cols != ''
		  BEGIN
				SET @PKDefinition = ', CONSTRAINT ' + @TargetObject + '_PK PRIMARY KEY (' + @Cols + ')';

		  END;
	 SET @TableStatement = REPLACE(@TableStatement, '#PrimaryKey#', @PKDefinition);

END;