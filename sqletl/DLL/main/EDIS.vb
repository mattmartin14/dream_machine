
Imports System.Text

Public Class EDIS

    Friend Shared info_msgs As New StringBuilder
    Friend Shared edis_params As New Dictionary(Of String, String)
    Friend Shared web_request_headers As New Dictionary(Of String, String)
    Friend Shared data_transfer_src_metadata As New Collection()
    Friend Shared _edis_err_msg As String = ""
    Friend Shared _exec_id As String = ""
    Friend Shared _mssql_inst As String = ""
    Friend Shared _is_pro_user As Boolean = False
    Friend Shared skip_data_transfer_log_row As Boolean = False
    ' Friend Shared debugger_on As Boolean = False

    Friend Shared Sub eMain()

        Try

            info_msgs.Append("<EDIS_INFO_MSGS>")
            info_msgs.Append(String.Format("<TASK_START_TS>{0}</TASK_START_TS>", EDIS.get_now_as_string))

            _exec_id = Main._exec_id
            _mssql_inst = Main._mssql_inst

            log_edis_event("info", "EDIS Main", "Start")

            Using cn As New SqlClient.SqlConnection("Server = " + _mssql_inst + "; Integrated Security = True")
                cn.Open()

                Using cmd As New SqlClient.SqlCommand(
                        "
                            SELECT itemID
                                ,CASE 
                                    WHEN lower(itemID) in ('src_cn_str','dest_cn_str','cn_str','ftp_uid','ftp_pwd','svc_uid','svc_pwd', 'client_secrets', 'proxy_svc_uid', 'proxy_svc_pwd')
                                    THEN DECRYPTBYPASSPHRASE(@p, itemVal)
                                    ELSE itemVal
                                END as itemVal
                                --,itemVal as original_val
                            FROM SSISDB.EDIS.util_task_params
                            WHERE exec_id = @p
                            --FROM [##" + _exec_id + "]
                        ", cn)
                    cmd.Parameters.AddWithValue("@p", _exec_id.ToString)
                    cmd.CommandTimeout = 0
                    Using reader = cmd.ExecuteReader
                        While reader.Read
                            If reader(0).ToString = "web_request_header" Then
                                'split on the delim

                                'this splits on multiple characters
                                Dim args As String() = reader(1).ToString.Split(New String() {"{|~}"}, StringSplitOptions.RemoveEmptyEntries)
                                'Dim args() = reader(1).ToString.Split("{|~}")
                                web_request_headers.Add(args(0), args(1))

                                log_edis_event("info", "EDIS Main", "web request header param [" + args(1) + "] added")

                            Else
                                Dim itemId As String = reader(0).ToString
                                Dim itemVal As String = ""
                                If reader(1) IsNot Nothing Then
                                    itemVal = reader(1).ToString
                                End If
                                edis_params.Add(itemId, itemVal)
                                'Dim orig_val As String = reader(2).ToString
                                Dim log_value As String
                                Select Case itemId.ToLower
                                    Case "install_id", "caller_auth", "src_cn_str", "dest_cn_str", "cn_str", "ftp_uid", "ftp_pwd", "svc_uid", "svc_pwd", "client_secrets", "proxy_svc_uid", "proxy_svc_pwd"
                                        log_value = "#######"
                                        'log_value = orig_val
                                    Case Else
                                        log_value = itemVal
                                End Select
                                log_edis_event("info", "EDIS Main", "EDIS Parameter [" + itemId + "] added with value [" + log_value + "]")
                            End If
                        End While
                        reader.Close()
                    End Using
                End Using

                'purge rec
                Using cmd2 As New SqlClient.SqlCommand("DELETE SSISDB.EDIS.util_task_params WHERE exec_id = @p", cn)
                    cmd2.Parameters.AddWithValue("@p", _exec_id.ToString)
                    cmd2.CommandTimeout = 0
                    cmd2.ExecuteNonQuery()
                End Using
                'Using cmd2 As New SqlClient.SqlCommand("DROP TABLE [##" + _exec_id + "]", cn)
                '    cmd2.ExecuteNonQuery()
                'End Using
                cn.Close()
                'If cn IsNot Nothing Then
                '    cn.Dispose()
                'End If
            End Using

            log_edis_event("info", "EDIS Main", "Finished Loading EDIS Parameters")

            'Check program

            'edit 2018-02-26: skipping pro check: company closed :(

            GoTo skip_pro_check

            Dim user_install_id As String = get_edis_pval("install_id")

            _is_pro_user = cSecurity.is_pro_user(_mssql_inst, user_install_id)

            If Not _is_pro_user Then
                Dim is_non_pro_err_msg As String = cSecurity.check_feature()

                If Not String.IsNullOrWhiteSpace(is_non_pro_err_msg) Then
                    _edis_err_msg = is_non_pro_err_msg
                    GoTo finish
                End If
            End If

skip_pro_check:

            _is_pro_user = True

            'cSecurity_OLD.check_sec_settings(_mssql_inst, user_install_id, _edis_err_msg)
            'If _edis_err_msg <> "" Then
            '    GoTo finish
            'End If

            log_edis_event("info", "EDIS Main", "user system phase 1 done")

            Dim task As String = LCase(EDIS.get_edis_pval("primary_task"))

            log_edis_event("info", "EDIS Main", "Primary Task:  [" + task + "]")

            Select Case task
                Case "data_transfer_task"
                    Call data_tsfr.data_tsfr_task(_edis_err_msg)
                Case "sql_cmd_task"
                    Call mSql_Cmd.Sql_Cmd_main(_edis_err_msg)
                Case "ftp_task"
                    cftp.ftp_main(_edis_err_msg)
                Case "file_task"
                    Call mLocalFile.local_file_main(_edis_err_msg)
                Case "zip_task"
                    Call cZip.zip_main(_edis_err_msg)
                Case "process_task"
                    Call cProcessTask.exec_process_task(_edis_err_msg)
                Case "web_task"
                    EDISWBTasks.webTask_main(_edis_err_msg)
                Case "sharepoint_task"
                    cSharePoint.sharepoint_main(_edis_err_msg)
                Case "excel_task"
                    Call cExcelMain.main(_edis_err_msg)
                Case "flatfile_task"
                    cFlatFile_tsfr.main(_edis_err_msg)
                Case "bigquery_task"
                    cBigQuery.main(_edis_err_msg)
                Case "powershell_task"
                    cPowershell.run_powershell_script(_edis_err_msg)
                Case "email_task"
                    cEmail.send_email(_edis_err_msg)
                Case "ews_task"
                    cEWS.main(_edis_err_msg)
                Case Else
                    _edis_err_msg = "EDIS Internal Error: The task [" + task + "] was not recognized."
            End Select

finish:

        Catch ex As Exception
            _edis_err_msg = ex.Message
        Finally

            If _edis_err_msg <> "" Then
                log_err(_edis_err_msg)
            End If

            info_msgs.Append(String.Format("<TASK_END_TS>{0}</TASK_END_TS>", EDIS.get_now_as_string))
            info_msgs.Append("</EDIS_INFO_MSGS>")

            log_info_msgs(info_msgs.ToString())

        End Try

        'If err_msg = "" Then
        '    If Not dts Is Nothing Then
        '        dts.TaskResult = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success
        '        log_edis_event("info", "EDIS Main", "Finished with no errors")
        '    End If
        'Else
        '    If Not dts Is Nothing Then
        '        dts.Events.FireError(-1, "EDIS", err_msg, "", 0)
        '        dts.TaskResult = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        '        log_edis_event("error", "EDIS Main", "EDIS Finished with Error Message: " + err_msg)
        '    End If

        'End If

        'finish_with_fail:

        '        dts.Events.FireError(-1, "EDIS", err_msg, "", 0)
        '        dts.TaskResult = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure


    End Sub


    Friend Shared Sub log_rows_tsfr(ByVal rows As Long)
        Using cn As New SqlClient.SqlConnection("Server = " + _mssql_inst + "; Integrated Security = True")
            cn.Open()
            Using cmd As New SqlClient.SqlCommand("EXEC SSISDB.EDIS.isp_log_info @exec_id, @action, @val", cn)
                cmd.Parameters.AddWithValue("@exec_id", EDIS._exec_id)
                cmd.Parameters.AddWithValue("@action", "rows_tsfr")
                cmd.Parameters.AddWithValue("@val", rows.ToString)
                cmd.ExecuteNonQuery()
            End Using
        End Using
    End Sub

    Friend Shared Function get_now_as_string() As String
        Return Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt")
    End Function

    Friend Shared Sub log_info_msgs(ByVal info_msgs As String)
        Using cn As New SqlClient.SqlConnection("Server = " + _mssql_inst + "; Integrated Security = True")
            cn.Open()
            Using cmd As New SqlClient.SqlCommand("EXEC SSISDB.EDIS.isp_log_info @exec_id, @action, @val", cn)
                cmd.Parameters.AddWithValue("@exec_id", EDIS._exec_id)
                cmd.Parameters.AddWithValue("@action", "info_msgs")
                cmd.Parameters.AddWithValue("@val", info_msgs)
                cmd.ExecuteNonQuery()
            End Using
        End Using
    End Sub

    Friend Shared Function get_edis_pval(ByVal param_nm As String) As String

        If edis_params.ContainsKey(param_nm) Then
            Return edis_params.Item(param_nm).ToString
        Else
            Return ""
        End If

    End Function

    Friend Shared Sub update_edis_pval(ByVal param_nm As String, ByVal param_val As String)

        If edis_params.ContainsKey(param_nm) Then
            edis_params.Item(param_nm) = param_val
        Else
            edis_params.Add(param_nm, param_val)
        End If

    End Sub

    Friend Class c_data_transfer_src_meta
        Property col_nm As String
        Property dts_data_type As String

    End Class


End Class


