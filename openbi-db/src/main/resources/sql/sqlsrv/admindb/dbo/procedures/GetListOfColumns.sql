CREATE PROCEDURE dbo.GetListOfColumns
		@Database varchar(4000)
	 , @Schema varchar(4000)
	 , @Table varchar(4000)
	 , @Scope varchar(4000) = 'ALL'
	 , @ListType varchar(4000) = 'SIMPLE'
	 , @PrefixLeft varchar(4000) = 'trg'
	 , @PrefixRight varchar(4000) = 'src'
	 , @ExludedColumn varchar(4000) = 'ValidTo'
	 , @List varchar(8000)OUTPUT
AS
BEGIN
	 DECLARE
		 @ColumnName varchar(4000)
	  , @DataType varchar(4000)
	  , @DataLength int
	  , @DataPrecision int
	  , @DataScale int
	  , @FirstPKColumn varchar(4000)
		 -- Cursor to get list of columns
	  , @CursorFirstPKStatement nvarchar(4000) = 'DECLARE CursorFirstPKColumn
	  CURSOR GLOBAL FOR
		   SELECT c.name AS column_name
			  FROM ' + @Database + '.sys.schemas AS s
			  JOIN ' + @Database + '.sys.objects AS o
			    ON s.schema_id = o.schema_id
				AND s.name = ''' + @Schema + '''
			  JOIN ' + @Database + '.sys.columns AS c
			    ON o.object_id = c.object_id
				AND o.name = ''' + @Table + '''
LEFT OUTER JOIN ' + @Database + '.sys.index_columns AS ic
             ON ic.object_id = c.object_id
			   AND c.column_id = ic.column_id
LEFT OUTER JOIN ' + @Database + '.sys.indexes AS i
             ON i.object_id = ic.object_id
				AND i.index_id = ic.index_id
				AND i.is_primary_key = 1
            AND ic.key_ordinal = 1'
	  , @CursorStatement nvarchar(4000) = 'DECLARE CursorColumn
	  CURSOR GLOBAL FOR
		   SELECT c.name AS column_name
				  , t.name AS data_type
				  , c.max_length AS data_length
				  , c.precision AS data_precision
				  , c.scale AS data_scale
			  FROM ' + @Database + '.sys.schemas AS s
			  JOIN ' + @Database + '.sys.objects AS o
			    ON s.schema_id = o.schema_id
				AND s.name = ''' + @Schema + '''
			  JOIN ' + @Database + '.sys.columns AS c
			    ON o.object_id = c.object_id
				AND o.name = ''' + @Table + '''
			  JOIN ' + @Database + '.sys.types AS t
			    ON c.system_type_id = t.system_type_id
LEFT OUTER JOIN ' + @Database + '.sys.index_columns AS ic
             ON ic.object_id = c.object_id
			   AND c.column_id = ic.column_id
LEFT OUTER JOIN ' + @Database + '.sys.indexes AS i
             ON i.object_id = ic.object_id
				AND i.index_id = ic.index_id
				AND i.is_primary_key = 1
		    WHERE 0 = 0
          ' + CASE
					  WHEN @ExludedColumn IS NOT NULL
						  THEN ' AND c.name != ''' + @ExludedColumn + ''''
					  ELSE ''
				  END + CASE @Scope
							  WHEN 'PK'
								  THEN ' AND ic.key_ordinal IS NOT NULL '
							  WHEN 'NPK'
								  THEN ' AND ic.key_ordinal IS NULL '
							  ELSE ''
						  END;
	 -- Get first PK column
	 IF @ListType = 'CHANGE'
		  BEGIN
				  
				-- Excecute cursor declaration
				EXEC sp_executesql @CursorFirstPKStatement;
				-- Open cursor
				OPEN CursorFirstPKColumn;

				FETCH NEXT FROM CursorFirstPKColumn INTO @FirstPKColumn;

				CLOSE CursorFirstPKColumn;
				DEALLOCATE CursorFirstPKColumn;
		  END;

	 SET @List = '';
	 -- Excecute cursor declaration
	 EXEC sp_executesql @CursorStatement;
	 -- Open cursor
	 OPEN CursorColumn;

	 FETCH NEXT FROM CursorColumn INTO @ColumnName, @DataType, @DataLength, @DataPrecision, @DataScale;

	 -- First item
	 SET @List+=CASE @ListType
						WHEN 'SIMPLE'
							THEN @ColumnName
						WHEN 'PREFIX'
							THEN @PrefixLeft + '.' + @ColumnName
						WHEN 'IS_NULL'
							THEN @PrefixLeft + '.' + @ColumnName + ' IS NULL'
						WHEN 'IS_NOT_NULL'
							THEN @PrefixLeft + '.' + @ColumnName + ' IS NOT NULL'
						WHEN 'SET'
							THEN @PrefixLeft + '.' + @ColumnName + ' = ' + @PrefixRight + '.' + @ColumnName
						WHEN 'EQUAL'
							THEN @PrefixLeft + '.' + @ColumnName + ' = ' + @PrefixRight + '.' + @ColumnName
						WHEN 'NOT_EQUAL'
							THEN @PrefixLeft + '.' + @ColumnName + ' != ' + @PrefixRight + '.' + @ColumnName 
								  --
								  + ' OR ' + @PrefixLeft + '.' + @ColumnName + ' IS NOT NULL AND ' + @PrefixRight + '.' + @ColumnName + ' IS NULL '
								  --
								  + ' OR ' + @PrefixLeft + '.' + @ColumnName + ' IS NULL AND ' + @PrefixRight + '.' + @ColumnName + ' IS NOT NULL '
						WHEN 'ISNULL'
							THEN 'ISNULL(' + @PrefixLeft + '.' + @ColumnName + ',' + @PrefixRight + '.' + @ColumnName + ') AS ' + @ColumnName
						WHEN 'CHANGE'
							THEN 'CASE WHEN ' + @PrefixRight + '.' + @FirstPKColumn + ' IS NULL THEN ' + @PrefixLeft + '.' + @ColumnName + ' ELSE ' + @PrefixRight + '.' + @ColumnName + ' END AS ' + @ColumnName
						WHEN 'DEFINITION'
							THEN @ColumnName + ' ' + @DataType + CASE
																				 WHEN @DataType LIKE '%date%'
																					OR @DataType LIKE '%int%'
																					 THEN ''
																				 ELSE CASE
																							WHEN @DataPrecision > 0
																								THEN '(' + CONVERT(varchar, @DataPrecision) + ',' + CONVERT(varchar, @DataScale) + ')'
																							ELSE '(' + CASE
																											  WHEN @DataLength < 0
																												  THEN 'max'
																											  ELSE CONVERT(varchar, @DataLength)
																										  END + ')'
																						END
																			 END
					END;

	 -- Loop for all other items
	 WHILE @@FETCH_STATUS = 0
		  BEGIN
				FETCH NEXT FROM CursorColumn INTO @ColumnName, @DataType, @DataLength, @DataPrecision, @DataScale;
				IF @@FETCH_STATUS = 0
					 BEGIN
						  SET @List+=CASE @ListType
											 WHEN 'SIMPLE'
												 THEN ',' + @ColumnName
											 WHEN 'PREFIX'
												 THEN ' ,' + @PrefixLeft + '.' + @ColumnName
											 WHEN 'IS_NULL'
												 THEN ' AND ' + @PrefixLeft + '.' + @ColumnName + ' IS NULL'
											 WHEN 'IS_NOT_NULL'
												 THEN ' AND ' + @PrefixLeft + '.' + @ColumnName + ' IS NOT NULL'
											 WHEN 'SET'
												 THEN ' , ' + @PrefixLeft + '.' + @ColumnName + ' = ' + @PrefixRight + '.' + @ColumnName
											 WHEN 'EQUAL'
												 THEN ' AND ' + @PrefixLeft + '.' + @ColumnName + ' = ' + @PrefixRight + '.' + @ColumnName
											 WHEN 'NOT_EQUAL'
												 THEN ' OR ' + @PrefixLeft + '.' + @ColumnName + ' != ' + @PrefixRight + '.' + @ColumnName 
														--
														+ ' OR ' + @PrefixLeft + '.' + @ColumnName + ' IS NOT NULL AND ' + @PrefixRight + '.' + @ColumnName + ' IS NULL '
														--
														+ ' OR ' + @PrefixLeft + '.' + @ColumnName + ' IS NULL AND ' + @PrefixRight + '.' + @ColumnName + ' IS NOT NULL '
											 WHEN 'ISNULL'
												 THEN ',ISNULL(' + @PrefixLeft + '.' + @ColumnName + ',' + @PrefixRight + '.' + @ColumnName + ') AS ' + @ColumnName
											 WHEN 'CHANGE'
												 THEN ', CASE WHEN ' + @PrefixRight + '.' + @FirstPKColumn + ' IS NULL THEN ' + @PrefixLeft + '.' + @ColumnName + ' ELSE ' + @PrefixRight + '.' + @ColumnName + ' END AS ' + @ColumnName
											 WHEN 'DEFINITION'
												 THEN ', ' + @ColumnName + ' ' + @DataType + CASE
																												WHEN @DataType LIKE '%date%'
																												  OR @DataType LIKE '%int%'
																													THEN ''
																												ELSE CASE
																														  WHEN @DataPrecision > 0
																															  THEN '(' + CONVERT(varchar, @DataPrecision) + ',' + CONVERT(varchar, @DataScale) + ')'
																														  ELSE '(' + CASE
																																			 WHEN @DataLength < 0
																																				 THEN 'max'
																																			 ELSE CONVERT(varchar, @DataLength)
																																		 END + ')'
																													  END
																											END
										 END;
					 END;
		  END;

	 CLOSE CursorColumn;
	 DEALLOCATE CursorColumn;
END;