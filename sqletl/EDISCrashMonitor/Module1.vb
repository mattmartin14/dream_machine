Imports System.Threading
Module Module1

    Sub Main(args() As String)

        'log_debug_event("start", "info", exec_id)

        ''debug
        'ReDim args(3)
        'args(0) = "MDDT\DEV16"
        'args(1) = "101379"
        'args(2) = "8888"
        'args(3) = "tmp123"

        Dim mssql_inst As String = args(0)
        Dim ssisdb_execution_id As Integer = CInt(args(1))
        Dim edis_process_id As Integer = CInt(args(2))
        Dim exec_id As String = args(3)

        Try


            log_debug_event("parameters binded", "info", exec_id)

            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True; Initial Catalog = SSISDB")
                cn.Open()
                log_debug_event("connection opened", "info", exec_id)
                Dim sql As String =
                        "
                    SELECT 
                        CASE WHEN end_time is null then 'RUNNING'
                            WHEN end_time is not null and status = 6 then 'ABORTED'
                            WHEN end_time is not null and status <> 6 then 'DONE'
                        END AS RUN_STATUS
                    FROM SSISDB.Catalog.executions WITH (NOLOCK)
                    WHERE execution_id = @ssisdb_execution_id
                    OPTION (MAXDOP 1)
                "
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@ssisdb_execution_id", ssisdb_execution_id)
                    log_debug_event("command parameter added...entering loop", "info", exec_id)
                    While 1 = 1

                        'poll every 5 seconds
                        Dim status = cmd.ExecuteScalar()

                        If status = "RUNNING" Then
                            log_debug_event("sleeping", "info", exec_id)
                            'sleep 5 seconds
                            Thread.Sleep(5000)
                        ElseIf status = "ABORTED" Then

                            log_debug_event("getting process handle instance", "info", exec_id)
                            Dim p = Process.GetProcessById(edis_process_id)
                            If Not p Is Nothing Then
                                log_debug_event("process acquired", "info", exec_id)
                                'ensure this is edis and not a recycled process id for something else!
                                'MDDataTechEDIS.exe
                                log_debug_event("process name is " + p.ProcessName, "info", exec_id)

                                If (p.ProcessName = "SQLETL_EDIS" Or p.ProcessName = "SQLETL_EDIS32") Then
                                    log_debug_event("killing process", "info", exec_id)
                                    p.Kill()
                                    log_debug_event("process killed...disposing", "info", exec_id)
                                    p.Dispose()
                                    log_debug_event("process disposed", "info", exec_id)
                                End If

                                Dim update_sql As String = "
                                    EXEC EDIS.isp_log_info @exec_id = @exec_id, @action = 'aborted', @val = ''
                                    
                                "
                                Using cmd2 As New SqlClient.SqlCommand(update_sql, cn)
                                    cmd2.Parameters.AddWithValue("@exec_id", exec_id)
                                    cmd2.ExecuteNonQuery()
                                    log_debug_event("updated aborted query rec", "info", exec_id)
                                End Using

                            End If
                            log_debug_event("exiting loop from abort phase", "info", exec_id)
                            Exit While

                        Else
                            log_debug_event("process had normal ending, no termination needed", "info", exec_id)
                            Exit While
                        End If

                    End While
                End Using
                cn.Close()
            End Using
        Catch ex As Exception

            'log_err_msg(exec_id, mssql_inst, "Crash Handler: " + ex.Message.ToString)

            'log_debug_event(ex.Message.ToString(), "error", exec_id)
        End Try


    End Sub

    'Sub log_err_msg(exec_id As String, mssql_inst As String, err_msg As String)
    '    Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True; Initial Catalog = SSISDB")
    '        cn.Open()
    '        Dim sql As String = "EXEC EDIS.isp_log_info @exec_id = @exec_id, @action = 'err_msg', @val = @err_msg"
    '        Using cmd As New SqlClient.SqlCommand(sql, cn)
    '            cmd.Parameters.AddWithValue("@err_msg", err_msg)
    '        End Using
    '    End Using
    'End Sub

    Sub log_debug_event(msg As String, typ As String, exec_id As String)
        Exit Sub
        Using cn As New SqlClient.SqlConnection("Server = ######; Integrated Security = True")
            cn.Open()
            Using cmd As New SqlClient.SqlCommand("INSERT EDIS_SANDBOX.dbo.crash_handler_log values (sysdatetime(), @exec_id, @typ, @msg)", cn)
                cmd.Parameters.AddWithValue("@exec_id", exec_id)
                cmd.Parameters.AddWithValue("@typ", typ)
                cmd.Parameters.AddWithValue("@msg", msg)
                cmd.ExecuteNonQuery()
            End Using
        End Using
    End Sub

End Module
