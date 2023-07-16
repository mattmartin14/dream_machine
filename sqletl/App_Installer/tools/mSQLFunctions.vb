Imports System.Data.SqlClient

Module mSQLFunctions

    'This path is guaranteed to have write access by the sql engine service account. if running backups or writeouts from t-sql, use this path
    Function mssql_temp_write_path(ByVal mssql_inst As String, ByRef msg As String) As String

        Try
            Dim res As String
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()

                Dim sql As String = <![CDATA[
                    select 
	                    replace(left(cast(value_data as varchar(1000))
                                        ,charindex('\Binn\',cast(value_data as varchar(1000)))),'"','')+'Log\' as data_path
                    from sys.dm_server_registry
                    where value_name= 'imagePath'
	                    and registry_key like 'HKLM\SYSTEM\CurrentControlSet\Services\MSSQL%'
                ]]>.Value

                sql = "SELECT SERVERPROPERTY('InstanceDefaultLogPath') as data_path"

                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    res = cmd.ExecuteScalar()
                End Using
                cn.Close()
            End Using
            Return res
        Catch ex As Exception
            msg = ex.Message.ToString
        End Try

    End Function

    Function check_for_legacy_svc_tbl(ByVal mssql_inst As String, ByRef err_msg As String) As String

        Dim legacy_tbl_msg As String = ""
        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                Dim legacy_table As String
                Dim sql As String = "select isnull((select table_name from ssisdb.information_schema.tables where left(table_name,11) = 'lkup_svc_id'),'') as does_tbl_exist"
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    legacy_table = cmd.ExecuteScalar().ToString
                    If legacy_table <> "" Then
                        legacy_tbl_msg += "Please note: Your version of EDIS contains a legacy table SSISDB.EDIS.[" + legacy_table + "] which requires a one time re-load of the connections."
                        legacy_tbl_msg += vbCrLf
                        legacy_tbl_msg += "This reload is only required for FTP, web, BigQuery, and SharePoint connections. All other connections do not need to be updated."
                        legacy_tbl_msg += vbCrLf
                        legacy_tbl_msg += "Please see help sections on configuring service IDs for FTP, web, BigQuery, and SharePoint for assistance."
                        legacy_tbl_msg += vbCrLf
                        legacy_tbl_msg += "Once you have reloaded your connections, can can simply drop table SSISDB.EDIS.[" + legacy_table + "]."

                    End If
                End Using
            End Using
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try


        Return legacy_tbl_msg

    End Function

    Function server_exist(ByVal mssql_inst As String, ByRef msg As String) As Boolean

        Dim res As Boolean = False

        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                res = True
            End Using
        Catch ex As SqlException
            msg = ex.Message
        End Try

        Return res

    End Function

    Function get_server_version(ByVal mssql_inst As String) As Integer
        Dim res As Integer
        Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
            cn.Open()
            Dim sql As String = "select cast(left(cast(SERVERPROPERTY('productversion') as varchar(30)),2) as int) * 10 as vsn "
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                res = cmd.ExecuteScalar()
            End Using
        End Using
        Return res
    End Function

    Function get_sql_vsn_year(ByVal mssql_inst As String) As Integer
        Dim res As Integer
        Dim vsn As String = get_server_version(mssql_inst)
        Select Case vsn
            Case 110
                res = 2012
            Case 120
                res = 2014
            Case 130
                res = 2016
            Case 140
                res = 2017
        End Select
        Return res
    End Function

    Function get_mssql_inst_full_nm(ByVal mssql_inst As String) As String
        Dim res As String = ""
        Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
            cn.Open()
            Dim sql As String = "SELECT @@servername as srvr_nm"
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                res = cmd.ExecuteScalar()
            End Using
        End Using
        Return res
    End Function


    Function get_clr_version(ByVal mssql_inst As String) As Integer
        Dim res As Integer
        Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
            cn.Open()
            Dim sql As String = "select cast(replace(left(value,2),'v','') as int) as clr_version from sys.dm_clr_properties " +
                        "where name = 'version'"
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                res = cmd.ExecuteScalar()
            End Using
        End Using
        Return res
    End Function

    Function ssisdb_exists(ByVal mssql_inst As String) As Boolean

        Dim res As Boolean = False

        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                Dim sql As String = "select isnull((select 1 from sys.databases " +
                    "where upper(name) = 'SSISDB'),0) " +
                    "as ssisdb_found"
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    res = cmd.ExecuteScalar()
                End Using
            End Using
        Catch ex As SqlException
        End Try

        Return res

    End Function

    Function is_sysadmin(ByVal mssql_inst As String) As Boolean

        Dim res As Boolean = False

        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                Dim sql As String = "select IS_SRVROLEMEMBER('sysadmin') as sa_ind "
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    res = cmd.ExecuteScalar()
                End Using
            End Using
        Catch ex As SqlException
        End Try

        Return res

    End Function

    Function is_server_admin(ByVal mssql_inst As String) As Boolean

        Dim res As Boolean = False

        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                Dim sql As String = "select isnull(IS_MEMBER('BUILTIN\Administrators'),0) as is_admin "
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    res = cmd.ExecuteScalar()
                End Using
            End Using
        Catch ex As SqlException
        End Try

        Return res

    End Function


    Function server_ISS_dep_wiz_path(ByVal mssql_inst As String) As String
        Dim res As String
        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()

                Dim sql As String = <![CDATA[
                    select 
					    replace(substring(cast(value_data as varchar(1000)),1,
					    (charindex('Microsoft SQL Server\', cast(value_data as varchar(1000)))
						    + len('Microsoft SQL Server\'))-1
					    ),'"','')
					    +cast(cast(left(cast(SERVERPROPERTY('productversion') as varchar(30)),2) as int) * 10 as varchar(5))
					    +'\DTS\Binn\ISDeploymentWizard.exe'
                    from sys.dm_server_registry
                    where value_name= 'imagePath'
	                    and registry_key like 'HKLM\SYSTEM\CurrentControlSet\Services\MSSQL%'
                ]]>.Value

                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    res = cmd.ExecuteScalar()
                End Using

            End Using
        Catch ex As SqlException
        End Try

        Return res
    End Function

    Function server_default_data_dir(ByVal mssql_inst As String) As String
        Dim res As String
        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()

                Dim sql As String = <![CDATA[
                    select 
	                    replace(left(cast(value_data as varchar(1000))
                                        ,charindex('\Binn\',cast(value_data as varchar(1000)))),'"','')+'DATA\' as data_path
                    from sys.dm_server_registry
                    where value_name= 'imagePath'
	                    and registry_key like 'HKLM\SYSTEM\CurrentControlSet\Services\MSSQL%'
                ]]>.Value
                
                sql = "SELECT SERVERPROPERTY('InstanceDefaultDataPath') as data_path"

                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    res = cmd.ExecuteScalar()
                End Using

            End Using
        Catch ex As SqlException
        End Try

        Return res
    End Function

    Function get_server_machine_name(ByVal mssql_inst As String) As String
        Dim res As String

        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                Dim sql As String = "select SERVERPROPERTY('machinename') "
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    res = cmd.ExecuteScalar()
                End Using
            End Using
        Catch ex As SqlException
            Throw New Exception(ex.Message, ex)
        End Try

        Return res
    End Function

    Sub runSqlCmd(ByVal mssql_inst As String, ByVal db_nm As String, ByVal sql_cmd As String)

        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True; initial catalog = " + db_nm + ";")
                cn.Open()
                Using cmd As New SqlClient.SqlCommand(sql_cmd, cn)
                    cmd.ExecuteNonQuery()
                End Using
            End Using
        Catch ex As SqlException
            Throw New Exception(ex.Message, ex)
        End Try

    End Sub

End Module





