'Friend Class cTestDBTsfr

'    Friend Shared Sub get_src_cols_from_reader()


'        Try

'            Dim dt As New DataTable

'            Dim sql As String = "SELECT top 1000 * FROM bigdata.dbo.large_table"

'            Using cn As New SqlClient.SqlConnection("Server = MDDT\DEV16; Integrated Security = True")
'                cn.Open()
'                Using cmd As New SqlClient.SqlCommand(sql, cn)
'                    Using reader = cmd.ExecuteReader()
'                        dt = reader.GetSchemaTable
'                    End Using
'                End Using
'            End Using

'            For i = 0 To dt.Columns.Count - 1
'                Dim src_col As New cDataTypeParser.src_column_metadata
'                src_col.col_nm = dt.Columns(i).ColumnName
'                src_col.data_type = dt.Columns(i).DataType.ToString
'                src_col.max_len = dt.Columns(i).MaxLength
'                src_col.ord_pos = dt.Columns(i).Ordinal.ToString

'                Console.WriteLine("Column Name {0}; Data Type {1}; Ordinal Position {2}; Max Length {3}", src_col.col_nm, src_col.data_type, src_col.ord_pos, src_col.max_len)

'                ' src_cols.Add(src_col)
'            Next



'        Catch ex As Exception
'            Throw New Exception(ex.Message)
'        End Try
'    End Sub

'End Class
