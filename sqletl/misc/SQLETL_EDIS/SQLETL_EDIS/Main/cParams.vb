
Imports System.Data.SqlClient

Friend Class cParams
    Friend Shared _edis_params As New Dictionary(Of String, String)

    Shared Sub load_params(ByVal mssql_inst As String, ByVal exec_id As String, ByVal sString1 As String, ByVal sString2 As String)

        Using cn As New SqlClient.SqlConnection("Server = " + mssql_inst + "; Integrated Security = True")
            cn.Open()
            Dim sql As String = "SELECT * FROM [##" + exec_id + "]"
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                Using reader = cmd.ExecuteReader
                    While reader.Read
                        _edis_params.Add(reader(0).ToString, reader(1).ToString)
                    End While
                End Using
            End Using
            'ditch the temp table
            Using cmd As New SqlClient.SqlCommand("DROP TABLE [##" + exec_id + "]", cn)
                cmd.ExecuteNonQuery()
            End Using
        End Using

        'load the sensitive strings
        Dim task As String = get_param_val("primary_task")
        Dim sub_task As String = get_param_val("sub_task")

        If task = "excel" And sub_task = "export_data" Then
            _edis_params.Add("src_cn_str", sString1)
        End If

    End Sub

    Shared Function get_param_val(ByVal param_nm As String) As String
        If _edis_params.ContainsKey(param_nm) Then
            Return _edis_params.Item(param_nm).ToString.ToLower
        Else
            Return ""
        End If
    End Function

End Class
