

Imports Microsoft.SqlServer.Server
Imports System.Security.Principal
Imports System.Data.SqlClient
Friend Class cMain

    Public Shared Sub main(exec_id As String)

        'cannot outer declare these as public or friend/shared because they will persist after each session
        Dim err_msg As String = ""
        Dim edis_params As New Dictionary(Of String, String)
        Dim web_request_headers As New Dictionary(Of String, String)
        Dim task As String

        Try

            'load params
            Using cn As New SqlClient.SqlConnection("context connection = true")
                cn.Open()

                Dim sql As String =
                     "
                            SELECT itemID
                                ,CASE 
                                    WHEN lower(itemID) in ('src_cn_str','dest_cn_str','cn_str','ftp_uid','ftp_pwd','svc_uid','svc_pwd', 'client_secrets', 'proxy_svc_uid', 'proxy_svc_pwd')
                                    THEN DECRYPTBYPASSPHRASE(@p, itemVal)
                                    ELSE itemVal
                                END as itemVal
                            FROM #edis
                        "

                ' Dim sql As String = "SELECT * FROM #edis"
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@p", exec_id.ToString)
                    Using reader = cmd.ExecuteReader()
                        While reader.Read

                            If reader(0).ToString = "web_request_header" Then

                                'this splits on multiple characters
                                Dim args As String() = reader(1).ToString.Split(New String() {"{|~}"}, StringSplitOptions.RemoveEmptyEntries)
                                web_request_headers.Add(args(0), args(1))

                            Else
                                Dim itemId As String = reader(0).ToString
                                Dim itemVal As String = ""
                                If reader(1) IsNot Nothing Then
                                    itemVal = reader(1).ToString
                                End If
                                edis_params.Add(itemId, itemVal)

                            End If


                        End While
                        reader.Close()
                    End Using
                End Using
                task = get_edis_pval(edis_params, "primary_task").ToLower
                Using cmd2 As New SqlClient.SqlCommand("DROP TABLE #edis", cn)
                    cmd2.ExecuteNonQuery()
                End Using
                cn.Close()
            End Using



            Select Case task
                Case "file_task"
                    Call cFileTasks.main(edis_params, err_msg)
                Case "web_task"
                    cWebTasks.main(edis_params, web_request_headers, err_msg)
                Case "admin_task"
                    cAdminTasks.main(edis_params, err_msg)
                    'Case "data_transfer"
                    '    cExeLauncher.run_edis(edis_params, err_msg)
            End Select

            If err_msg <> "" Then
                log_err(exec_id, err_msg)
            End If

        Catch ex As Exception
            Throw New Exception(ex.Message.ToString)
            err_msg = ex.Message.ToString
            log_err(exec_id, err_msg)

        End Try

    End Sub

    Friend Shared Function get_edis_pval(params As Dictionary(Of String, String), ByVal param_nm As String) As String

        If params.ContainsKey(param_nm) Then
            Return params.Item(param_nm).ToString
        Else
            Return ""
        End If

    End Function

    Friend Shared Sub log_err(exec_id As String, err_msg As String)

        'truncate error message to left 4K characters
        err_msg = Left(err_msg, 4000)

        Using cn As New SqlClient.SqlConnection("context connection = true")
            cn.Open()
            Dim cmd_text As String = "EXEC SSISDB.EDIS.isp_log_info @exec_id, 'err_msg',  @err_msg"
            Using cmd As New SqlClient.SqlCommand(cmd_text, cn)

                cmd.Parameters.AddWithValue("@err_msg", err_msg)
                cmd.Parameters.AddWithValue("@exec_id", exec_id)

                cmd.ExecuteNonQuery()

            End Using
            cn.Close()
        End Using

    End Sub

    Friend Shared Function create_exec_id() As String
        Return Guid.NewGuid.ToString().ToUpper()
    End Function

    Friend Shared Sub impersonate_user(ByRef clientID As WindowsIdentity, ByRef impersonatedUser As WindowsImpersonationContext)
        clientID = SqlContext.WindowsIdentity
        impersonatedUser = clientID.Impersonate()
    End Sub

    Friend Shared Sub undo_impersonation(ByRef impersonatedUser As WindowsImpersonationContext)
        If impersonatedUser IsNot Nothing Then
            impersonatedUser.Undo()
        End If
    End Sub

    'could perm this through stored proc
    Friend Shared Sub log_task_finish(exec_id As String, outcome As String, err_msg As String)
        Using cn As New SqlConnection("context connection = true")
            cn.Open()
            'cn.ChangeDatabase("SSISDB")
            Dim sql As String = "
                SET NOCOUNT ON; 
                UPDATE EDIS.etl_audit 
                    set task_end_ts = getdate()
                        , task_duration = datediff([second], task_start_ts, getdate())
                        , task_run_outcome = @outcome, err_msg = @err_msg  
                WHERE exec_id = @exec_id
                OPTION (MAXDOP 1)"

            Using cmd As New SqlCommand(sql, cn)
                cmd.Parameters.AddWithValue("@exec_id", exec_id)
                cmd.Parameters.AddWithValue("@outcome", outcome)

                If err_msg IsNot Nothing Then
                    cmd.Parameters.AddWithValue("@err_msg", err_msg)
                Else
                    cmd.Parameters.AddWithValue("@err_msg", DBNull.Value)
                End If
                SqlContext.Pipe.ExecuteAndSend(cmd)
            End Using
        End Using
    End Sub

    Friend Shared Sub raiserror(msg As String, exec_id As String, task_nm As String)
        Try

            Dim url_link As String
            Select Case task_nm.ToLower
                Case "file_task - create_dir", "file_task - delete_dir"
                    url_link = "https://sqletl.com/online-user-guide/local-file-tasks-oug/"
                Case Else
                    url_link = ""
            End Select


            Dim err_msg As String = "*** EDIS Error ***" + vbCrLf
            err_msg += "Task [" + task_nm + "], Exec ID [" + exec_id + "] encountered an error. " + vbCrLf
            err_msg += "The detailed error message is below. " + vbCrLf
            err_msg += "For more information, please run query ""SELECT * FROM SSISDB.EDIS.etl_audit where exec_id = '" + exec_id + "'""" + vbCrLf
            If Not String.IsNullOrWhiteSpace(url_link) Then
                err_msg += "For user guide information, please visit this link: " + url_link + vbCrLf
            End If
            err_msg += "----- Begin Error Message -----" + vbCrLf
            err_msg += msg + vbCrLf
            err_msg += "----- End Error Message -----" + vbCrLf

            Using cn As New SqlConnection("context connection = True")
                cn.Open()
                Dim sql As String = "Raiserror(@err_msg, 16, 16) WITH NOWAIT"
                Using cmd As New SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@err_msg", err_msg)

                    'wrapping the error in a try catch keeps the error clean and not show all the .NET garbage stack messenging
                    'it also avoids a clr bubble up error
                    Try
                        SqlContext.Pipe.ExecuteAndSend(cmd)
                    Catch
                    End Try
                End Using
            End Using

        Catch ex As Exception
            Throw
        End Try
    End Sub

    Friend Shared Sub raiserror(msg As String)
        Try

            Using cn As New SqlConnection("context connection = True")
                cn.Open()
                Dim sql As String = "Raiserror(@err_msg, 16, 16) WITH NOWAIT"
                Using cmd As New SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@err_msg", msg)

                    'wrapping the error in a try catch keeps the error clean and not show all the .NET garbage stack messenging
                    'it also avoids a clr bubble up error
                    Try
                        SqlContext.Pipe.ExecuteAndSend(cmd)
                    Catch
                    End Try
                End Using
            End Using

        Catch ex As Exception
            Throw
        End Try
    End Sub

End Class
