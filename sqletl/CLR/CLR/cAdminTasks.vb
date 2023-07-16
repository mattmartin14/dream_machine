Friend Class cAdminTasks

    Friend Shared Sub main(ByVal params As Dictionary(Of String, String), ByRef err_msg As String)

        Dim sub_task As String = cMain.get_edis_pval(params, "sub_task").ToLower

        Select Case sub_task
            Case "setup_proxy"
                setup_proxy(params, err_msg)
            Case Else
                err_msg = "admin sub task not found"
        End Select
    End Sub

    Private Shared Sub setup_proxy(ByVal params As Dictionary(Of String, String), ByRef err_msg As String)
        Try

            Dim login_nm As String = cMain.get_edis_pval(params, "login_nm")
            Dim login_pwd As String = cMain.get_edis_pval(params, "login_pwd")
            Dim proxy_id As String = cMain.get_edis_pval(params, "proxy_id")

            Using cn As New SqlClient.SqlConnection("context connection = true")
                cn.Open()

                Dim sql As String = "EXEC SSISDB.EDIS.isp_setup_proxy_id @login_nm, @login_pwd, @proxy_id"
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@login_nm", login_nm)
                    cmd.Parameters.AddWithValue("@login_pwd", login_pwd)
                    cmd.Parameters.AddWithValue("@proxy_id", proxy_id)
                    cmd.ExecuteNonQuery()

                End Using

            End Using
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try
    End Sub

End Class
