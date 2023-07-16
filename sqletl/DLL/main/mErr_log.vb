Module mErr_log

    'log error message to audit table
    Sub log_err(ByRef err_msg As String)

        'truncate error message to left 4K characters
        '9/14...errors now are nvarchar(max)
        'err_msg = Left(err_msg, 4000)

        Using cn As New SqlClient.SqlConnection("Server = " + EDIS._mssql_inst + "; integrated security = true")
            cn.Open()
            Dim cmd_text As String = "EXEC SSISDB.EDIS.isp_log_info @exec_id, @action, @err_msg"
            Using cmd As New SqlClient.SqlCommand(cmd_text, cn)

                cmd.Parameters.AddWithValue("@exec_id", EDIS._exec_id)
                cmd.Parameters.AddWithValue("@action", "err_msg")
                cmd.Parameters.AddWithValue("@err_msg", err_msg)

                cmd.ExecuteNonQuery()

            End Using

        End Using

    End Sub

End Module