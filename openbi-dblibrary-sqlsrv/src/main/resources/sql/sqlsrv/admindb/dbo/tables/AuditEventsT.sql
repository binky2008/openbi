CREATE TABLE dbo.AuditEventsT (
				 event_time datetime NULL
			  , sequence_number int NOT NULL
			  , action_id varchar(4)NULL
			  , succeeded bit NOT NULL
			  , permission_bitmask bigint NOT NULL
			  , is_column_permission bit NOT NULL
			  , session_id smallint NOT NULL
			  , server_principal_id int NOT NULL
			  , database_principal_id int NOT NULL
			  , target_server_principal_id int NOT NULL
			  , target_database_principal_id int NOT NULL
			  , object_id int NOT NULL
			  , class_type varchar(2)NULL
			  , session_server_principal_name nvarchar(128)NULL
			  , server_principal_name nvarchar(128)NULL
			  , server_principal_sid varbinary(85)NULL
			  , database_principal_name nvarchar(128)NULL
			  , target_server_principal_name nvarchar(128)NULL
			  , target_server_principal_sid varbinary(85)NULL
			  , target_database_principal_name nvarchar(128)NULL
			  , server_instance_name nvarchar(128)NULL
			  , database_name nvarchar(128)NULL
			  , schema_name nvarchar(128)NULL
			  , object_name nvarchar(128)NULL
			  , statement nvarchar(4000)NULL
			  , additional_information nvarchar(4000)NULL
			  , file_name nvarchar(260)NOT NULL
			  , audit_file_offset bigint NOT NULL);