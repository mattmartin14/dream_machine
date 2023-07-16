Module mCI_Cols

    Function get_ci_cols(ByVal mssql_inst As String, ByVal full_tbl_nm As String, ByRef err_msg As String) As String

        Dim key_cols As String

        Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = sspi")

            Dim db_nm As String

            Dim sql_str As String = "select parsename(@full_tbl_nm,3) as db_nm"
            cn.Open()
            Using cmd As New SqlClient.SqlCommand(sql_str, cn)
                cmd.Parameters.AddWithValue("@full_tbl_nm", full_tbl_nm)
                db_nm = cmd.ExecuteScalar().ToString
            End Using

            sql_str = _
                "select col.name, ixc.key_ordinal as col_ord_pos " + _
                "from [" + db_nm + "].sys.tables as tb with (nolock) " + _
                    "inner join [" + db_nm + "].sys.columns as col with (nolock) " + _
                    "   on tb.object_id = col.object_id " + _
                    "inner join [" + db_nm + "].sys.indexes as ix with (nolock) " + _
                    "	on tb.object_id = ix.object_id and ix.is_primary_key = 1 and ix.type_desc = 'CLUSTERED' " + _
                    "inner join [" + db_nm + "].sys.index_columns as ixc with (nolock) " + _
                    "	on ix.object_id = ixc.object_id and ix.index_id = ixc.index_id and col.column_id = ixc.column_id " + _
                "where tb.object_id = object_id(@full_tbl_nm) " + _
                "order by ixc.key_ordinal "

            Using cmd2 As New SqlClient.SqlCommand(sql_str, cn)
                cmd2.Parameters.AddWithValue("@full_tbl_nm", full_tbl_nm)
                Dim reader As SqlClient.SqlDataReader
                reader = cmd2.ExecuteReader()

                While reader.Read

                    If key_cols = "" Then
                        key_cols = reader(0).ToString
                    Else
                        key_cols = key_cols + ", " + reader(0).ToString()
                    End If

                End While
                reader.Close()
                reader = Nothing
            End Using


        End Using


        If key_cols = "" Then
            err_msg = "Error getting clustered index key columns for clustered index load to target table [" & full_tbl_nm & "]. " & vbCrLf
            err_msg = err_msg & "Table does not have a clustered index. "
            Return Nothing
            Exit Function
        End If

        Return key_cols

    End Function

End Module