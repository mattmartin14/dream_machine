Module Module1

    'exe to run EDIS proc
    Public Sub Main(ByVal args As String())

        Try
            Dim tmp_tbl As String = args(0)
            Dim exec_guid As String = args(1)
            Dim mssql_inst As String = args(2)
            Dim proc As String

            Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True; Initial Catalog = SSISDB")
                cn.Open()

                Using cmd As New SqlClient.SqlCommand("SELECT ItemVal from " + tmp_tbl + " WHERE ItemID = 'proc_nm'", cn)
                    proc = cmd.ExecuteScalar.ToString
                End Using

                Dim mddt_cmd As New SqlClient.SqlCommand
                mddt_cmd.CommandText = proc
                mddt_cmd.Connection = cn
                mddt_cmd.CommandTimeout = 0
                mddt_cmd.CommandType = CommandType.StoredProcedure

                'read vars

                Using cmd As New SqlClient.SqlCommand("SELECT * FROM " + tmp_tbl + " WHERE ItemID not in ('proc_nm','proxy_id')", cn)
                    Dim rd As SqlClient.SqlDataReader
                    rd = cmd.ExecuteReader
                    While rd.Read
                        Dim curr_param As String = rd.Item("ItemID")

                        If rd.Item("ItemVal") IsNot Nothing Then
                            mddt_cmd.Parameters.AddWithValue(curr_param, rd.Item("ItemVal").ToString)
                        Else
                            mddt_cmd.Parameters.AddWithValue(curr_param, DBNull.Value)
                        End If

                    End While

                    rd.Close()

                End Using

                'add execution proxy flag
                mddt_cmd.Parameters.AddWithValue("@proxy_id", "{MDDT_PX_EXEC_GUID}" + exec_guid)

                mddt_cmd.ExecuteNonQuery()

                cn.Close()

            End Using
        Catch ex As Exception
            Throw New System.Exception(ex.Message.ToString())
        End Try

    End Sub

End Module
