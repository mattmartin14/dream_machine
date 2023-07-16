
'Need to add reference to System.ServiceProcess

Imports System.Data.SqlClient
Imports System.Threading
Imports System.IO
Imports System
Imports System.Security.Principal
Imports Microsoft.Win32

Public Class cSecChecks

    Public Shared Sub run_sec_checks(ByVal mssql_inst As String, ByRef err_msg As String, ByVal check_action As String)
        security_checks(mssql_inst, err_msg, check_action)
    End Sub

    'ensures user has rights to install the program
    Private Shared Sub security_checks(ByVal mssql_inst As String, ByRef err_msg As String, ByVal check_action As String)

        'checks in UI before getting here
        If does_server_exist(mssql_inst) = False Then
            err_msg = "Server [" + mssql_inst + "] was not accessible. Ensure you are able to connect."
            Exit Sub
        End If

        Dim sql_version As String = ""
        If is_SQL_Version_Supported(mssql_inst, sql_version) = False Then
            err_msg = "Note: The version of SQL Server you are attempting to " + check_action + " EDIS on is unsupported."
            Exit Sub
        End If

        If is_sysadmin(mssql_inst) = False Then
            err_msg = "Your user ID is not a sysadmin on server [" + mssql_inst + "] . In order to " + check_action + " EDIS, you must be a sysadmin."
            Exit Sub
        End If
        If is_local_admin(mssql_inst) = False Then
            err_msg = "Your user ID is not a Local Admin on the Windows Server hosting [" + mssql_inst + "] . In order to " + check_action + " EDIS, you must be a local admin."
            Exit Sub
        End If

        If is_running_as_admin() = False Then
            err_msg = "Please Note: In order to " + check_action + " this program, you must be running in Administrator Mode. Please close the installer, and right click on setup.exe and choose 'Run as Administrator'"
            Exit Sub
        End If

        If is_on_machine(mssql_inst) = False Then
            err_msg = "In order to " + check_action + " EDIS, you must be logged onto the Windows Server host [" + System.Environment.MachineName + "]."
            Exit Sub
        End If
        If is_ssisdb_installed(mssql_inst) = False Then
            err_msg = "In order to " + check_action + " EDIS, the SSIS Catalog and SSISDB needs to be installed. For more information on installing the SSIS catalog, please see link: "
            err_msg += vbCrLf
            err_msg += "https://msdn.microsoft.com/en-us/library/gg471509.aspx"
            Exit Sub
        End If
        If is_ssis_service_running(mssql_inst) = False Then
            err_msg = "The SSIS Service for [" + mssql_inst + "] is not running. Ensure the service is running in order to " + check_action + " EDIS"
            Exit Sub
        End If

        If check_if_dot_net_45_or_above_installed() = False Then
            err_msg = "EDIS requires .NET Framework 4.5 or higher. Please download and install prior to continuing."
            Exit Sub
        End If


    End Sub

    Private Shared Function check_if_dot_net_45_or_above_installed() As Boolean

        Dim is_installed As Boolean = False

        Dim key_path As String = "SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full\"

        Using netKey As RegistryKey = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry32).OpenSubKey(key_path)

            Dim releaseKey As Integer = Convert.ToInt32(netKey.GetValue("Release"))

            '4.5 is key 378,389
            If releaseKey >= 378389 Then
                is_installed = True
            End If

        End Using

        Return is_installed

    End Function

    Private Shared Function is_SQL_Version_Supported(ByVal mssql_inst As String, ByRef sql_version As String) As Boolean

        'Current versions supported:
        '   [1] 110 - SQL 2012
        '   [2] 120 - SQL 2014
        '   [3] 130 - SQL 2016
        '   [4] 140 - SQL 2017

        sql_version = mSQLFunctions.get_server_version(mssql_inst)

        Dim res As Boolean = False
        Select Case sql_version
            Case 110, 120, 130, 140
                res = True
            Case Else
                res = False
        End Select

        'If sql_version = 110 Then
        '    res = True
        'ElseIf sql_version = 120 Then
        '    res = True
        'ElseIf sql_version = 130 Then
        '    res = True
        'End If

        Return res

    End Function


    Private Shared Function is_running_as_admin() As Boolean

        Dim identity = WindowsIdentity.GetCurrent()
        Dim principal = New WindowsPrincipal(identity)
        Return principal.IsInRole(WindowsBuiltInRole.Administrator)

    End Function


    Private Shared Function does_server_exist(ByVal mssql_inst As String) As Boolean

        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
            End Using
            Return True
        Catch ex As SqlException
            Return False
        End Try

    End Function

    Private Shared Function is_sysadmin(ByVal mssql_inst As String) As Boolean
        Try
            Dim res As Boolean = False
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                Dim sql As String = "select IS_SRVROLEMEMBER('sysadmin') as sa_ind "
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    res = cmd.ExecuteScalar()
                End Using
                Return res
            End Using
        Catch ex As SqlException
            Return False
        End Try
    End Function

    Private Shared Function is_local_admin(ByVal mssql_inst As String) As Boolean
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

    Private Shared Function is_on_machine(ByVal mssql_inst As String) As Boolean
        Dim res As Boolean = False
        Dim sql_machine_nm As String
        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                Dim sql As String = "select SERVERPROPERTY('machinename') "
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    sql_machine_nm = cmd.ExecuteScalar()
                End Using
            End Using

            If sql_machine_nm = System.Environment.MachineName Then
                res = True
            End If

        Catch ex As SqlException
            Throw New Exception(ex.Message, ex)
        End Try

        Return res

    End Function

    Private Shared Function is_ssisdb_installed(ByVal mssql_inst As String) As Boolean

        Dim res As Boolean = False

        Try
            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
                cn.Open()
                Dim sql As String = "select case when db_id('SSISDB') is null then 0 else 1 end as is_ssidb_installed"
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    res = cmd.ExecuteScalar()
                End Using
            End Using
        Catch ex As Exception
        End Try

        Return res
    End Function

    Private Shared Function is_ssis_service_running(ByVal mssql_inst As String) As Boolean

        Dim res As Boolean = False

        'get server version
        Dim server_version As String

        Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
            cn.Open()
            Dim sql As String = "select cast(left(cast(SERVERPROPERTY('productversion') as varchar(30)),2) as int) * 10 as vsn "
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                server_version = cmd.ExecuteScalar()
            End Using
        End Using

        '---------------------------------------------------------------------------------------------------
        'service shows up as MsDtsServer1##
        Dim vsn_to_search As String = "msdtsserver" & server_version

        Dim services() As System.ServiceProcess.ServiceController
        Dim i As Integer
        services = System.ServiceProcess.ServiceController.GetServices()
        For i = 0 To services.Length - 1

            Dim svc_nm As String = LCase(services(i).ServiceName)

            If (svc_nm = vsn_to_search) Then

                Dim svc_status As String = services(i).Status.ToString.ToLower

                If svc_status = "running" Then res = True

                Exit For

            End If

        Next

        Return res

    End Function

End Class
