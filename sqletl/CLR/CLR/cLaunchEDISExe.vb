Imports Microsoft.SqlServer.Server
Imports System.Security.Principal
Imports System.Threading

Public Class cLaunchEDISExe



    'NOTE: TO do this right will take significant work
    ' 1) will have to use the advapi.dll to do the following:
    '       a. duplicate the primary user token
    '       b. create an enviroment block
    '       c. run createProcessAsUser
    '           this requires a lot of flags to get set
    '       d. run this process inside a diff domain...suspend main thread when it starts. when done, resume thread
    '       e. implement safe handle and mutex to ensure the resources are not shared across other processes

    '   when this is done, can probably convert the run_process_task to be here.. and its either a user running a process
    ' or it runs the EDIS.exe
    '       not sure on the createprocessas user how to capture outputs or error messages from the output
    '       looks like i will have to overload a class and impliment an interface


    'the current setup below to start a new process reverts back to running as the sql account even though i've set impersonation

    <SqlProcedure>
    Public Shared Sub edis_task_exe(ByVal edis_path As String, ByVal mssql_inst As String, ByVal exec_id As String, ByVal logging_level As String)


        Dim p As New Process
        Dim user_cancel As Boolean = False
        Dim err_msg As String
        Dim userName As String

        'sets caller as the one launching this
        Dim impersonatedUser As WindowsImpersonationContext = Nothing
        Dim clientID As WindowsIdentity = Nothing

        userName = Environment.UserDomainName + "\" + Environment.UserName


        Try
            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            Dim args As String = String.Format("""{0}"" ""{1}"" ""{2}""", mssql_inst, exec_id, logging_level)

            With p.StartInfo
                .FileName = edis_path
                .Arguments = args
                'redirection and shellexec as false do not work in CLR, since the it shares the same AppDomain...you have to shell exec out
                ' and as long as the shell program has a good error handling/logging process, you should be fine
                .UseShellExecute = True
                .WindowStyle = ProcessWindowStyle.Hidden
                .CreateNoWindow = True
            End With

            p.Start()

            p.WaitForExit()

            If p.ExitCode <> 0 Then
                Throw New Exception("Failure to launch EDIS")
            End If

        Catch ab As ThreadAbortException
            'they say sometimes this event wont fire and will get skipped
            user_cancel = True
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        Finally
            'cancellations get caught here...no need to have a crash monitor??
            If p IsNot Nothing Then
                If Not p.HasExited Then
                    Dim info_msg As String
                    Try
                        p.Kill()
                        info_msg = "<EDIS_INFO_MSGS>Process aborted - cancel level (K)</EDIS_INFO_MSGS>"
                        log_info_msgs(info_msg, mssql_inst, exec_id)
                    Catch ex As Exception
                        p.CloseMainWindow()
                        info_msg = "<EDIS_INFO_MSGS>Process aborted - cancel level (CMW)</EDIS_INFO_MSGS>"
                        log_info_msgs(info_msg, mssql_inst, exec_id)
                    End Try

                End If
                p.Dispose()
                p = Nothing
            End If

            'undo the impersonation
            If impersonatedUser IsNot Nothing Then
                impersonatedUser.Undo()
            End If

            'log after undo impersonation since it uses a context connection
            If Not String.IsNullOrWhiteSpace(err_msg) Then
                cMain.log_err(exec_id, err_msg)
                'log the error message if its not already logged from EDIS exe
            End If

            'sqlcontext only works after user impersonation has been undone
            If user_cancel Then
                Dim abort_msg As String = "EDIS task cancelled by user [" + userName + "]"
                SqlContext.Pipe.Send(abort_msg)
            End If

        End Try
    End Sub

    Friend Shared Sub log_info_msgs(ByVal info_msgs As String, mssql_inst As String, exec_id As String)
        Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
            cn.Open()
            Using cmd As New SqlClient.SqlCommand("EXEC SSISDB.EDIS.isp_log_info @exec_id, @action, @val", cn)
                cmd.Parameters.AddWithValue("@exec_id", exec_id)
                cmd.Parameters.AddWithValue("@action", "info_msgs")
                cmd.Parameters.AddWithValue("@val", info_msgs)
                cmd.ExecuteNonQuery()
            End Using
        End Using
    End Sub

End Class
