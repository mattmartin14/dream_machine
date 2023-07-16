
	-- EDIS Uninstall Scripts

	-- =======================================================================================================================================
	-- Drop Procs, tables, views, functions

	declare @cmd nvarchar(max)
	select @cmd = isnull(@cmd,'')+
		concat('if object_id(''',object_schema_name(object_id),'.',name,''') is not null drop '
			,case type 
				when 'U' then 'table '
				when 'P' then 'proc '
				WHEN 'PC' then 'proc '
				when 'FN' then 'function '
				when 'FS' then 'function '
				when 'FT' then 'function '
				WHEN 'V' then 'View '
				WHEN 'SN' THEN 'SYNONYM '
				WHEN 'AF' then 'AGGREGATE '
			end
			--iif(type = 'U', 'table ', 'proc ')
			,object_schema_name(object_id),'.',name
		) +'; '
	from sys.objects
	where OBJECT_SCHEMA_NAME(object_id) IN('EDIS','MDDataTech')
		and type in( 'P','FN','V','U','PC','FS','SN','FT','AF')
	;

	exec SSISDB..sp_executesql @cmd

	GO

	-- ====================================================================================================================================
	-- Assembly objects to drop

	-- Assembly
	IF EXISTS(SELECT * FROM sys.assemblies WHERE NAME = 'MDDataTechCLR')
		BEGIN
			DROP ASSEMBLY [MDDataTechCLR]
		END

	GO

	IF EXISTS(SELECT * FROM sys.assemblies WHERE NAME = 'SQLETL_EDIS')
		BEGIN
			DROP ASSEMBLY SQLETL_EDIS
		END

	GO

	-- Assembly SSISDB User
	IF EXISTS(SELECT * FROM SYS.DATABASE_PRINCIPALS WHERE NAME = 'MDDataTech_EDIS_AssemblySigner')
		BEGIN
			DROP USER [MDDataTech_EDIS_AssemblySigner]
		END

	GO

	-- Assembly SSISDB User
	IF EXISTS(SELECT * FROM SYS.DATABASE_PRINCIPALS WHERE NAME = 'SQLETL_EDIS_AssemblySigner')
		BEGIN
			DROP USER [SQLETL_EDIS_AssemblySigner]
		END

	GO

	-- Assembly Login
	--{CHANGE DATABASE:MASTER}
	IF EXISTS(SELECT * FROM SYS.SERVER_PRINCIPALS WHERE NAME = 'MDDataTech_EDIS_AssemblySigner')
		BEGIN
			DROP LOGIN [MDDataTech_EDIS_AssemblySigner]
		END

	GO

	-- Assembly Login
	--{CHANGE DATABASE:MASTER}
	IF EXISTS(SELECT * FROM SYS.SERVER_PRINCIPALS WHERE NAME = 'SQLETL_EDIS_AssemblySigner')
		BEGIN
			DROP LOGIN [SQLETL_EDIS_AssemblySigner]
		END

	GO

	-- Assymetric key
	--{CHANGE DATABASE:MASTER}
	IF EXISTS(SELECT * FROM sys.asymmetric_keys WHERE NAME = 'MDDataTech_EDIS_AssemblyLoadingKey')
		BEGIN
			DROP ASYMMETRIC KEY MDDataTech_EDIS_AssemblyLoadingKey
		END

	GO

	-- Assymetric key
	--{CHANGE DATABASE:MASTER}
	IF EXISTS(SELECT * FROM sys.asymmetric_keys WHERE NAME = 'SQLETL_EDIS_AssemblyLoadingKey')
		BEGIN
			DROP ASYMMETRIC KEY SQLETL_EDIS_AssemblyLoadingKey
		END

	GO

	-- =======================================================================================================================================
	-- DROP TYPES
	declare @cmd_types nvarchar(max);

	select @cmd_types = isnull(@cmd_types,'')+'DROP TYPE ['+SCHEMA_NAME(schema_id)+'].['+name+']; '
	from sys.types
	WHERE schema_name(schema_id) in('MDDataTech','EDIS')
	;

	exec SSISDB..sp_executesql @cmd_types

	GO

	-- ========================================================================================================================================
	-- Schemas

	IF EXISTS(SELECT 1 FROM SYS.SCHEMAS WHERE NAME = 'MDDataTech')
		BEGIN
			exec SSISDB..sp_executesql N'DROP SCHEMA [MDDataTech]'
		END
	GO

	IF EXISTS(SELECT 1 FROM SYS.SCHEMAS WHERE NAME = 'EDIS')
		BEGIN
			exec SSISDB..sp_executesql N'DROP SCHEMA [EDIS]'
		END
	GO


	-- ========================================================================================================================================
	-- Users/Logins/Certificates/MSDB Procs

	--{CHANGE DATABASE:MSDB}
	IF exists(select * from sys.procedures where OBJECT_SCHEMA_NAME(object_id) = 'dbo' and name = 'usp_MDDataTech_EDIS_run_pkg')
		DROP PROCEDURE dbo.usp_MDDataTech_EDIS_run_pkg
	GO

	--{CHANGE DATABASE:MSDB}
	IF exists(select * from sys.procedures where OBJECT_SCHEMA_NAME(object_id) = 'dbo' and name = 'usp_SQLETL_EDIS_run_pkg')
		DROP PROCEDURE dbo.usp_SQLETL_EDIS_run_pkg
	GO

	--{CHANGE DATABASE:MSDB}
	IF exists(select * from sys.procedures where OBJECT_SCHEMA_NAME(object_id) = 'dbo' and name = 'usp_MDDataTech_EDIS_get_sys_info')
		DROP PROCEDURE dbo.usp_MDDataTech_EDIS_get_sys_info
	GO

	--{CHANGE DATABASE:MSDB}
	IF exists(select * from sys.procedures where OBJECT_SCHEMA_NAME(object_id) = 'dbo' and name = 'usp_SQLETL_EDIS_get_sys_info')
		DROP PROCEDURE dbo.usp_SQLETL_EDIS_get_sys_info
	GO

	IF EXISTS(SELECT * FROM sys.certificates WHERE name = 'MDDataTechEDISProxyCert')
		DROP CERTIFICATE MDDataTechEDISProxyCert

	GO

	IF EXISTS(SELECT * FROM sys.certificates WHERE name = 'SQLETL_EDIS_ProxyCert')
		DROP CERTIFICATE SQLETL_EDIS_ProxyCert

	GO

	--{CHANGE DATABASE:MSDB}
	IF EXISTS(SELECT * FROM sys.database_principals where name = 'MDDataTechEDISOperator')
		DROP USER MDDataTechEDISOperator

	GO

	--{CHANGE DATABASE:MSDB}
	IF EXISTS(SELECT * FROM sys.database_principals where name = 'SQLETL_EDIS_Operator')
		DROP USER SQLETL_EDIS_Operator

	GO

	--{CHANGE DATABASE:MASTER}
	IF EXISTS(SELECT * FROM sys.database_principals where name = 'MDDataTechEDISOperator')
		DROP USER MDDataTechEDISOperator

	GO

	--{CHANGE DATABASE:MASTER}
	IF EXISTS(SELECT * FROM sys.database_principals where name = 'SQLETL_EDIS_Operator')
		DROP USER SQLETL_EDIS_Operator

	GO

	--{CHANGE DATABASE:MASTER}
	IF EXISTS(SELECT * FROM sys.server_principals where name = 'MDDataTechEDISOperator')
		DROP LOGIN MDDataTechEDISOperator

	GO

	--{CHANGE DATABASE:MASTER}
	IF EXISTS(SELECT * FROM sys.server_principals where name = 'SQLETL_EDIS_Operator')
		DROP LOGIN SQLETL_EDIS_Operator

	GO

	--{CHANGE DATABASE:MASTER}
	IF EXISTS(SELECT * FROM sys.certificates WHERE name = 'MDDataTechEDISProxyCert')
		DROP CERTIFICATE MDDataTechEDISProxyCert

	GO

	--{CHANGE DATABASE:MASTER}
	IF EXISTS(SELECT * FROM sys.certificates WHERE name = 'SQLETL_EDIS_ProxyCert')
		DROP CERTIFICATE SQLETL_EDIS_ProxyCert

	GO

	IF EXISTS(SELECT 1 FROM SYS.SYMMETRIC_KEYS WHERE NAME = 'MDDataTechSymKey')
            BEGIN
                DROP SYMMETRIC KEY [MDDataTechSymKey]
            END

	GO

	IF EXISTS(SELECT 1 FROM SYS.SYMMETRIC_KEYS WHERE NAME = 'SQLETL_EDIS_SKEY')
            BEGIN
                DROP SYMMETRIC KEY [SQLETL_EDIS_SKEY]
            END

	GO

	IF EXISTS(SELECT 1 FROM SYS.CERTIFICATES WHERE NAME = 'MDDataTechCert')
        BEGIN
            DROP CERTIFICATE [MDDataTechCert]
        END

	GO

	IF EXISTS(SELECT 1 FROM SYS.CERTIFICATES WHERE NAME = 'SQLETL_EDIS_CERT')
        BEGIN
            DROP CERTIFICATE [SQLETL_EDIS_CERT]
        END

	GO


	-- ========================================================================================================================================
	-- SSIS Catalog


	-- This enviroment only existed on the mddatatech schema and is no longer valid
	IF EXISTS(
			select 1
			from SSISDB.catalog.environments as e
				inner join SSISDB.catalog.folders as f
					on e.folder_id = f.folder_id
			where f.name = 'MDDataTech' and e.name = 'EDIS_Env'
		
		)
		BEGIN
			EXEC [SSISDB].[catalog].[delete_environment] @environment_name=N'EDIS_Env', @folder_name=N'MDDataTech'
		END

	GO
		
	IF EXISTS(
		SELECT 1 
        FROM SSISDB.CATALOG.PROJECTS AS P
            INNER JOIN SSISDB.CATALOG.FOLDERS AS F
                ON P.FOLDER_ID = F.FOLDER_ID
        WHERE P.NAME = N'EDIS' AND F.NAME = N'MDDataTech')
    BEGIN
        EXEC [SSISDB].[catalog].[delete_project] @FOLDER_NAME = N'MDDataTech', @PROJECT_NAME  = N'EDIS'
    END

	IF EXISTS(
		SELECT 1 
        FROM SSISDB.CATALOG.PROJECTS AS P
            INNER JOIN SSISDB.CATALOG.FOLDERS AS F
                ON P.FOLDER_ID = F.FOLDER_ID
        WHERE P.NAME = N'EDIS' AND F.NAME = N'SQLETL.COM')
    BEGIN
        EXEC [SSISDB].[catalog].[delete_project] @FOLDER_NAME = N'SQLETL.COM', @PROJECT_NAME  = N'EDIS'
    END
	
	IF EXISTS(SELECT * FROM SSISDB.catalog.folders WHERE NAME = 'MDDataTech')
		BEGIN 
			EXEC [SSISDB].[catalog].[delete_folder] @folder_name=N'MDDataTech' ;
		END  

	IF EXISTS(SELECT * FROM SSISDB.catalog.folders WHERE NAME = 'SQLETL.COM')
		BEGIN 
			EXEC [SSISDB].[catalog].[delete_folder] @folder_name=N'SQLETL.COM' ;
		END  


	GO

	-- rename if its the legacy prior to removal
	IF EXISTS(
	select *
	from sys.database_principals
	where name = 'MDDataTech_Role'
		and type = 'R'
	)
	BEGIN

		ALTER ROLE MDDataTech_Role with Name = [EDIS_Role]

	END

	GO

	-- DB Role (EDIS)
	IF EXISTS(SELECT * FROM SYS.database_principals WHERE type_desc = 'DATABASE_ROLE' AND NAME = 'EDIS_Role')
		BEGIN

			DECLARE @RoleName sysname
			set @RoleName = N'EDIS_Role'

			IF @RoleName <> N'public' and (select is_fixed_role from sys.database_principals where name = @RoleName) = 0
			BEGIN
				DECLARE @RoleMemberName sysname
				DECLARE Member_Cursor CURSOR FOR
				select [name]
				from sys.database_principals 
				where principal_id in ( 
					select member_principal_id
					from sys.database_role_members
					where role_principal_id in (
						select principal_id
						FROM sys.database_principals where [name] = @RoleName AND type = 'R'))

				OPEN Member_Cursor;

				FETCH NEXT FROM Member_Cursor
				into @RoleMemberName
	
				DECLARE @SQL NVARCHAR(4000)

				WHILE @@FETCH_STATUS = 0
				BEGIN
		
					SET @SQL = 'ALTER ROLE '+ QUOTENAME(@RoleName,'[') +' DROP MEMBER '+ QUOTENAME(@RoleMemberName,'[')
					EXEC(@SQL)
		
					FETCH NEXT FROM Member_Cursor
					into @RoleMemberName
				END;

				CLOSE Member_Cursor;
				DEALLOCATE Member_Cursor;
			END
			
			-- Remove Members

			DROP ROLE [EDIS_Role]
		END


