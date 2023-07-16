
Imports System.IO
Imports System.Text

'Notes: Do not make data readers dynamic objects...it slows down performance a ton
Friend Class cFlatFile_tsfr

    Shared textcolList As New HashSet(Of Integer)

    Shared Sub main(ByRef err_msg As String)
        Select Case EDIS.get_edis_pval("sub_task").ToLower
            Case "export_data"
                Dim src_qry As String = EDIS.get_edis_pval("src_qry")
                Dim row_term As String = EDIS.get_edis_pval("row_term")
                Dim text_qual As String = EDIS.get_edis_pval("text_qual")
                Dim col_delim As String = EDIS.get_edis_pval("col_delim")
                Dim append_to_existing As String = EDIS.get_edis_pval("append_to_existing_file")
                Dim include_headers As String = EDIS.get_edis_pval("include_headers")
                Dim src_provider As String = EDIS.get_edis_pval("src_provider")
                Dim cn_str As String = EDIS.get_edis_pval("cn_str")
                Dim encoding_str As String = EDIS.get_edis_pval("encoding_str")
                Dim file_path As String = EDIS.get_edis_pval("file_path")
                Select Case src_provider.ToLower
                    Case "mssql"
                        export_data_mssql(file_path, src_qry, cn_str, col_delim, row_term, text_qual, include_headers, append_to_existing, encoding_str, err_msg)
                    Case "oledb"
                        export_data_oledb(file_path, src_qry, cn_str, col_delim, row_term, text_qual, include_headers, append_to_existing, encoding_str, err_msg)
                    Case "odbc"
                        export_data_odbc(file_path, src_qry, cn_str, col_delim, row_term, text_qual, include_headers, append_to_existing, encoding_str, err_msg)

                End Select

        End Select
    End Sub

    'NOTE: We have 3 copies of this export (ODBC, OLEDB, and MSSQL).
    '   - The reason we are repeating this code 3x over is because the speed when the writer knows the reader type (mssql,odbc,oledb) improves signifcantly versus modulating this
    '       - and using IDatareader

    Friend Shared Sub export_data_mssql(ByVal file_path As String, ByVal src_qry As String, ByVal cn_str As String,
                                   ByVal col_delim As String, ByVal row_term As String, ByVal text_qual As String,
                                   ByVal include_headers As Boolean, ByVal append_to_existing_file As Boolean, ByVal encoding_str As String,
                                   ByRef err_msg As String)

        'Dim curr_col As String
        Try

            log_edis_event("info", "Flat file export", "parameters set")

            'update column delimiter and row term
            col_delim = set_delim(col_delim)
            row_term = set_delim(row_term)

            log_edis_event("info", "Flat file export", "delimeters updated")

            Dim enc_format As Encoding = set_encoder(encoding_str)
            Dim _row_index As Integer = 0

            log_edis_event("info", "Flat file export", "setting connection")

            Using cn As New SqlClient.SqlConnection(cn_str)
                cn.Open()
                Using cmd As New SqlClient.SqlCommand(src_qry, cn)
                    cmd.CommandTimeout = 0
                    Using reader As SqlClient.SqlDataReader = cmd.ExecuteReader()
                        log_edis_event("info", "Flat file export", "reader opened")
                        If reader.HasRows Then

                            'blow away if we are not appending
                            If Not append_to_existing_file Then
                                If File.Exists(file_path) Then
                                    File.Delete(file_path)
                                End If
                            End If

                            Dim field_cnt As Integer = reader.FieldCount - 1

                            Dim bufferSize As Integer = 65536
                            Dim buffer_pad As Integer = bufferSize * 1.1
                            Dim data As New StringBuilder(buffer_pad)

                            Using fs As New IO.FileStream(file_path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, bufferSize)

                                log_edis_event("info", "Flat file export", "file created...starting export")

                                Dim loop_counter As Integer = 0
                                Using sw As New StreamWriter(fs)

                                    '-------------------------------------------
                                    'get header meta
                                    Dim src_cols As New List(Of src_metadata)

                                    src_cols = load_src_cols(reader)

                                    log_edis_event("info", "Flat file export", "source meta data added")

                                    'add headers
                                    If include_headers Then
                                        data.Append(src_cols(0).col_nm)
                                        'sw.Write(src_cols(0).col_nm)
                                        For i = 1 To src_cols.Count - 1
                                            data.Append(col_delim + src_cols(i).col_nm)
                                            'sw.Write(col_delim + src_cols(i).col_nm)
                                        Next
                                        data.Append(row_term)
                                        'sw.Write(row_term)
                                        log_edis_event("info", "Flat file export", "headers added")
                                    End If

                                    'write data
                                    Dim using_text_qual As Boolean = False

                                    If Not String.IsNullOrEmpty(text_qual) Then
                                        using_text_qual = True
                                    End If

                                    'we will run a seperate loop if text qualifiers are involved so we dont have to re-evaluate every time
                                    If using_text_qual Then

                                        'text qualifier reader
                                        Do While reader.Read

                                            If textcolList.Contains(0) Then
                                                'If src_cols(0).is_text Then
                                                data.Append(text_qual + get_sql_val(reader, 0, src_cols(0).data_type) + text_qual)
                                                'data.Append(text_qual + reader(0).ToString + text_qual)
                                            Else
                                                data.Append(get_sql_val(reader, 0, src_cols(0).data_type))
                                            End If
                                            For i = 1 To field_cnt
                                                'add the delmiter
                                                If textcolList.Contains(i) Then
                                                    data.Append(col_delim + text_qual + get_sql_val(reader, i, src_cols(i).data_type) + text_qual)
                                                Else
                                                    'data.Append(reader(i).ToString)
                                                    data.Append(col_delim + get_sql_val(reader, i, src_cols(i).data_type))
                                                End If
                                            Next
                                            data.Append(row_term)

                                            'flush buffer if we hit threshold
                                            If data.Length >= bufferSize Then
                                                sw.Write(data)
                                                data.Clear()
                                            End If


                                            'update counters
                                            _row_index += 1
                                            loop_counter += 1

                                            If loop_counter >= 10000 Then
                                                log_edis_event("info", "Flat file export", "10k rows written")
                                                loop_counter = 0
                                            End If
                                        Loop
                                    Else
                                        'no text qualifier
                                        Do While reader.Read
                                            'add first col
                                            'curr_col = src_cols(0).col_nm
                                            get_sql_val(reader, 0, src_cols(0).data_type)
                                            For i As Integer = 1 To field_cnt
                                                ' curr_col = src_cols(i).col_nm
                                                data.Append(col_delim + get_sql_val(reader, i, src_cols(i).data_type))
                                            Next i
                                            data.Append(row_term)

                                            'flush buffer if we hit threshold
                                            If data.Length >= bufferSize Then
                                                sw.Write(data)
                                                data.Clear()
                                            End If

                                            'update counters
                                            _row_index += 1
                                            loop_counter += 1

                                            If loop_counter >= 10000 Then
                                                log_edis_event("info", "Flat file export", "10k rows written")
                                                loop_counter = 0
                                            End If

                                        Loop
                                    End If

                                    'final write
                                    If data.Length >= 1 Then
                                        sw.Write(data)
                                        data.Clear()
                                    End If

                                    log_edis_event("info", "flat file write complete", _row_index.ToString)

                                End Using
                            End Using
                        End If
                    End Using
                    log_edis_event("info", "Flat file export", "reader closed")
                End Using
            End Using

            EDIS.log_rows_tsfr(_row_index)

            log_edis_event("info", "Flat file export", "rows transferred logged")

        Catch ex As Exception
            'err_msg = curr_col + " > " + ex.Message.ToString()
            err_msg = ex.Message
        End Try


    End Sub

    Friend Shared Sub export_data_oledb(ByVal file_path As String, ByVal src_qry As String, ByVal cn_str As String,
                                   ByVal col_delim As String, ByVal row_term As String, ByVal text_qual As String,
                                   ByVal include_headers As Boolean, ByVal append_to_existing_file As Boolean, ByVal encoding_str As String,
                                   ByRef err_msg As String)

        'Dim curr_col As String

        Try

            log_edis_event("info", "Flat file export", "parameters set")

            'update column delimiter and row term
            col_delim = set_delim(col_delim)
            row_term = set_delim(row_term)

            log_edis_event("info", "Flat file export", "delimeters updated")

            Dim enc_format As Encoding = set_encoder(encoding_str)
            Dim _row_index As Integer = 0

            log_edis_event("info", "Flat file export", "setting connection")

            Using cn As New OleDb.OleDbConnection(cn_str)
                cn.Open()
                Using cmd As New OleDb.OleDbCommand(src_qry, cn)
                    cmd.CommandTimeout = 0
                    Using reader As OleDb.OleDbDataReader = cmd.ExecuteReader(CommandBehavior.SequentialAccess)
                        log_edis_event("info", "Flat file export", "reader opened")
                        If reader.HasRows Then

                            'blow away if we are not appending
                            If Not append_to_existing_file Then
                                If File.Exists(file_path) Then
                                    File.Delete(file_path)
                                End If
                            End If

                            Dim field_cnt As Integer = reader.FieldCount - 1

                            Dim bufferSize As Integer = 65536
                            Dim buffer_pad As Integer = bufferSize * 1.1
                            Dim data As New StringBuilder(buffer_pad)

                            Using fs As New IO.FileStream(file_path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, bufferSize)

                                log_edis_event("info", "Flat file export", "file created...starting export")

                                Dim loop_counter As Integer = 0
                                Using sw As New StreamWriter(fs)

                                    '-------------------------------------------
                                    'get header meta
                                    Dim src_cols As New List(Of src_metadata)

                                    src_cols = load_src_cols(reader)

                                    log_edis_event("info", "Flat file export", "source meta data added")

                                    'add headers
                                    If include_headers Then
                                        data.Append(src_cols(0).col_nm)
                                        'sw.Write(src_cols(0).col_nm)
                                        For i = 1 To src_cols.Count - 1
                                            data.Append(col_delim + src_cols(i).col_nm)
                                            'sw.Write(col_delim + src_cols(i).col_nm)
                                        Next
                                        data.Append(row_term)
                                        'sw.Write(row_term)
                                        log_edis_event("info", "Flat file export", "headers added")
                                    End If

                                    'write data
                                    Dim using_text_qual As Boolean = False

                                    If Not String.IsNullOrEmpty(text_qual) Then
                                        using_text_qual = True
                                    End If

                                    'we will run a seperate loop if text qualifiers are involved so we dont have to re-evaluate every time
                                    If using_text_qual Then

                                        'text qualifier reader
                                        Do While reader.Read

                                            If textcolList.Contains(0) Then
                                                'If src_cols(0).is_text Then
                                                data.Append(text_qual + get_oledb_val(reader, 0, src_cols(0).data_type) + text_qual)
                                                'data.Append(text_qual + reader(0).ToString + text_qual)
                                            Else
                                                data.Append(get_oledb_val(reader, 0, src_cols(0).data_type))
                                            End If
                                            For i = 1 To field_cnt
                                                'add the delmiter
                                                If textcolList.Contains(i) Then
                                                    data.Append(col_delim + text_qual + get_oledb_val(reader, i, src_cols(i).data_type) + text_qual)
                                                Else
                                                    'data.Append(reader(i).ToString)
                                                    data.Append(col_delim + get_oledb_val(reader, i, src_cols(i).data_type))
                                                End If
                                            Next
                                            data.Append(row_term)

                                            'flush buffer if we hit threshold
                                            If data.Length >= bufferSize Then
                                                sw.Write(data)
                                                data.Clear()
                                            End If


                                            'update counters
                                            _row_index += 1
                                            loop_counter += 1

                                            If loop_counter >= 10000 Then
                                                log_edis_event("info", "Flat file export", "10k rows written")
                                                loop_counter = 0
                                            End If
                                        Loop
                                    Else
                                        'no text qualifier
                                        Do While reader.Read
                                            'add first col
                                            ' curr_col = src_cols(0).col_nm
                                            get_oledb_val(reader, 0, src_cols(0).data_type)
                                            For i As Integer = 1 To field_cnt
                                                'curr_col = src_cols(i).col_nm
                                                data.Append(col_delim + get_oledb_val(reader, i, src_cols(i).data_type))
                                            Next i
                                            data.Append(row_term)

                                            'flush buffer if we hit threshold
                                            If data.Length >= bufferSize Then
                                                sw.Write(data)
                                                data.Clear()
                                            End If

                                            'update counters
                                            _row_index += 1
                                            loop_counter += 1

                                            If loop_counter >= 10000 Then
                                                log_edis_event("info", "Flat file export", "10k rows written")
                                                loop_counter = 0
                                            End If

                                        Loop
                                    End If

                                    'final write
                                    If data.Length >= 1 Then
                                        sw.Write(data)
                                        data.Clear()
                                    End If

                                    log_edis_event("info", "flat file write complete", _row_index.ToString)

                                End Using
                            End Using
                        End If
                    End Using
                    log_edis_event("info", "Flat file export", "reader closed")
                End Using
            End Using

            EDIS.log_rows_tsfr(_row_index)

            log_edis_event("info", "Flat file export", "rows transferred logged")

        Catch ex As Exception
            err_msg = ex.Message
        End Try


    End Sub

    Friend Shared Sub export_data_odbc(ByVal file_path As String, ByVal src_qry As String, ByVal cn_str As String,
                                   ByVal col_delim As String, ByVal row_term As String, ByVal text_qual As String,
                                   ByVal include_headers As Boolean, ByVal append_to_existing_file As Boolean, ByVal encoding_str As String,
                                   ByRef err_msg As String)
        'Dim curr_col As String

        Try

            log_edis_event("info", "Flat file export", "parameters set")

            'update column delimiter and row term
            col_delim = set_delim(col_delim)
            row_term = set_delim(row_term)

            log_edis_event("info", "Flat file export", "delimeters updated")

            Dim enc_format As Encoding = set_encoder(encoding_str)
            Dim _row_index As Integer = 0

            log_edis_event("info", "Flat file export", "setting connection")

            Using cn As New Odbc.OdbcConnection(cn_str)
                cn.Open()
                Using cmd As New Odbc.OdbcCommand(src_qry, cn)
                    cmd.CommandTimeout = 0
                    Using reader As Odbc.OdbcDataReader = cmd.ExecuteReader(CommandBehavior.SequentialAccess)
                        log_edis_event("info", "Flat file export", "reader opened")
                        If reader.HasRows Then

                            'blow away if we are not appending
                            If Not append_to_existing_file Then
                                If File.Exists(file_path) Then
                                    File.Delete(file_path)
                                End If
                            End If

                            Dim field_cnt As Integer = reader.FieldCount - 1

                            Dim bufferSize As Integer = 65536
                            Dim buffer_pad As Integer = bufferSize * 1.1
                            Dim data As New StringBuilder(buffer_pad)

                            Using fs As New IO.FileStream(file_path, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, bufferSize)

                                log_edis_event("info", "Flat file export", "file created...starting export")

                                Dim loop_counter As Integer = 0
                                Using sw As New StreamWriter(fs)

                                    '-------------------------------------------
                                    'get header meta
                                    Dim src_cols As New List(Of src_metadata)

                                    src_cols = load_src_cols(reader)

                                    log_edis_event("info", "Flat file export", "source meta data added")

                                    'add headers
                                    If include_headers Then
                                        data.Append(src_cols(0).col_nm)
                                        'sw.Write(src_cols(0).col_nm)
                                        For i = 1 To src_cols.Count - 1
                                            data.Append(col_delim + src_cols(i).col_nm)
                                            'sw.Write(col_delim + src_cols(i).col_nm)
                                        Next
                                        data.Append(row_term)
                                        'sw.Write(row_term)
                                        log_edis_event("info", "Flat file export", "headers added")
                                    End If

                                    'write data
                                    Dim using_text_qual As Boolean = False

                                    If Not String.IsNullOrEmpty(text_qual) Then
                                        using_text_qual = True
                                    End If

                                    'we will run a seperate loop if text qualifiers are involved so we dont have to re-evaluate every time
                                    If using_text_qual Then

                                        'text qualifier reader
                                        Do While reader.Read

                                            If textcolList.Contains(0) Then
                                                'If src_cols(0).is_text Then
                                                data.Append(text_qual + get_odbc_val(reader, 0, src_cols(0).data_type) + text_qual)
                                                'data.Append(text_qual + reader(0).ToString + text_qual)
                                            Else
                                                data.Append(get_odbc_val(reader, 0, src_cols(0).data_type))
                                            End If
                                            For i = 1 To field_cnt
                                                'add the delmiter
                                                If textcolList.Contains(i) Then
                                                    data.Append(col_delim + text_qual + get_odbc_val(reader, i, src_cols(i).data_type) + text_qual)
                                                Else
                                                    'data.Append(reader(i).ToString)
                                                    data.Append(col_delim + get_odbc_val(reader, i, src_cols(i).data_type))
                                                End If
                                            Next
                                            data.Append(row_term)

                                            'flush buffer if we hit threshold
                                            If data.Length >= bufferSize Then
                                                sw.Write(data)
                                                data.Clear()
                                            End If


                                            'update counters
                                            _row_index += 1
                                            loop_counter += 1

                                            If loop_counter >= 10000 Then
                                                log_edis_event("info", "Flat file export", "10k rows written")
                                                loop_counter = 0
                                            End If
                                        Loop
                                    Else
                                        'no text qualifier
                                        Do While reader.Read
                                            'add first col
                                            'curr_col = src_cols(0).col_nm
                                            get_odbc_val(reader, 0, src_cols(0).data_type)
                                            For i As Integer = 1 To field_cnt
                                                ' curr_col = src_cols(i).col_nm
                                                data.Append(col_delim + get_odbc_val(reader, i, src_cols(i).data_type))
                                            Next i
                                            data.Append(row_term)

                                            'flush buffer if we hit threshold
                                            If data.Length >= bufferSize Then
                                                sw.Write(data)
                                                data.Clear()
                                            End If

                                            'update counters
                                            _row_index += 1
                                            loop_counter += 1

                                            If loop_counter >= 10000 Then
                                                log_edis_event("info", "Flat file export", "10k rows written")
                                                loop_counter = 0
                                            End If

                                        Loop
                                    End If

                                    'final write
                                    If data.Length >= 1 Then
                                        sw.Write(data)
                                        data.Clear()
                                    End If

                                    log_edis_event("info", "flat file write complete", _row_index.ToString)

                                End Using
                            End Using
                        End If
                    End Using
                    log_edis_event("info", "Flat file export", "reader closed")
                End Using
            End Using

            EDIS.log_rows_tsfr(_row_index)

            log_edis_event("info", "Flat file export", "rows transferred logged")

        Catch ex As Exception
            err_msg = ex.Message
        End Try


    End Sub

    Private Shared Function load_src_cols(ByVal reader As IDataReader) As List(Of src_metadata)

        'Dim cols As New Collection()
        Dim cols As New List(Of src_metadata)

        For i = 0 To reader.FieldCount - 1
            Dim is_text As Boolean = False
            Dim data_type As String = reader.GetDataTypeName(i).ToLower
            Select Case data_type
                Case "varchar", "nvarchar", "char", "nchar", "string", "text", "ntext",
                      "dbtype_wchar", "dbtype_wvarchar", "dbtype_wlongvarchar",
                     "dbtype_char", "dbtype_varchar", "dbtype_longvarchar"
                    is_text = True
            End Select
            If is_text Then
                textcolList.Add(i)
            End If
            Dim col As New src_metadata With {.col_nm = reader.GetName(i), .ord_pos = i, .is_text = is_text, .data_type = data_type}

            cols.Add(col)
            log_edis_event("info", "Flat file export", "meta data for column [" + col.col_nm + "] added, datatype = [" + data_type + "]")
        Next

        Return cols

    End Function


    Private Shared Function set_delim(ByVal delim_text As String) As String

        If LCase(Left(delim_text, 7)) = "ascii::" Then
            Dim char_val_str As String = Replace(LCase(delim_text), "ascii::", "")
            Dim char_vals() As String = Split(char_val_str, ";")
            Dim delim_fmt As String = ""
            For i As Integer = LBound(char_vals) To UBound(char_vals)
                delim_fmt += Chr(char_vals(i)).ToString()
            Next
            Return delim_fmt
        Else
            Select Case delim_text.ToLower
                Case "{crlf}", "{cr}{lf}"
                    Return vbCrLf
                Case "{cr}"
                    Return vbCr
                Case "{lf}"
                    Return vbLf
                Case "{tab}"
                    Return vbTab
                Case "{bs}"
                    Return vbBack
                Case Else
                    Return delim_text
            End Select
        End If

    End Function

    Private Shared Function set_encoder(ByVal encoding_str As String) As Encoding
        Select Case encoding_str.ToLower
            Case "utf7"
                Return Encoding.UTF7
            Case "utf8"
                Return Encoding.UTF8
            Case "utf32"
                Return Encoding.UTF32
            Case "unicode"
                Return Encoding.Unicode
            Case "bigendianunicode"
                Return Encoding.BigEndianUnicode
            Case "ascii"
                Return Encoding.ASCII
            Case Else
                Return Encoding.Default
        End Select
    End Function


    Private Shared Function get_sql_val(ByVal reader As SqlClient.SqlDataReader, ByVal ord_pos As Integer, ByVal sqltype As String) As String

        If reader.IsDBNull(ord_pos) Then Return ""

        'get the type
        Select Case sqltype
            Case "varchar", "nvarchar", "char", "nchar", "text", "ntext"
                Return reader.GetSqlString(ord_pos)
            Case "int", "integer"
                Return reader.GetSqlInt32(ord_pos).ToString
            Case "numeric", "decimal"
                Return reader.GetSqlDecimal(ord_pos).ToString
            Case "datetime"
                Return reader.GetSqlDateTime(ord_pos).ToString '("yyyy-MM-dd hh:mm:ss.fff")
            Case "float"
                Return reader.GetSqlDouble(ord_pos).ToString
            Case "bigint"
                Return reader.GetSqlInt64(ord_pos).ToString
            Case "smallint"
                Return reader.GetSqlInt16(ord_pos).ToString
            Case "tinyint"
                Return reader.GetSqlByte(ord_pos).ToString
            Case "real"
                Return reader.GetFloat(ord_pos).ToString
            Case "bit"
                Return reader.GetSqlBoolean(ord_pos).ToString()
            Case "money"
                Return reader.GetSqlMoney(ord_pos).ToString()
            Case "date"
                Return reader.GetDateTime(ord_pos).ToString("yyyy-MM-dd")
            Case "datetime2"
                Return reader.GetDateTime(ord_pos).ToString()
            Case "datetimeoffset"
                Return reader.GetDateTimeOffset(ord_pos).ToString()
            Case "varbinary", "binary", "image"
                Return reader.GetSqlBytes(ord_pos).ToString()
            Case "uniqueidentifier"
                Return reader.GetSqlGuid(ord_pos).ToString()
            Case Else
                'Return ""
                'Return reader(ord_pos).ToString()
                Return reader.GetValue(ord_pos).ToString()
        End Select

    End Function

    Private Shared Function get_oledb_val(ByVal reader As OleDb.OleDbDataReader, ByVal ord_pos As Integer, ByVal datatype As String) As String

        If reader.IsDBNull(ord_pos) Then Return ""

        'get the type
        Select Case datatype
            Case "varchar", "nvarchar", "char", "nchar", "string", "text", "ntext",
                      "dbtype_wchar", "dbtype_wvarchar", "dbtype_wlongvarchar",
                     "dbtype_char", "dbtype_varchar", "dbtype_longvarchar"
                Return reader.GetString(ord_pos)
            Case "int", "integer", "dbtype_i4"
                Return reader.GetInt32(ord_pos).ToString
            Case "numeric", "decimal", "dbtype_numeric"
                Return reader.GetDecimal(ord_pos).ToString
            Case "datetime", "timestamp", "dbtype_dbtimestamp"
                Return reader.GetDateTime(ord_pos).ToString '("yyyy-MM-dd hh:mm:ss.fff")
            Case "float", "double"
                Return reader.GetDouble(ord_pos).ToString
            Case "bigint"
                Return reader.GetInt64(ord_pos).ToString
            Case "smallint", "dbtype_i2"
                Return reader.GetInt16(ord_pos).ToString
            Case "tinyint"
                Return reader.GetByte(ord_pos).ToString
            Case "real"
                Return reader.GetFloat(ord_pos).ToString
            Case "bit", "boolean"
                Return reader.GetBoolean(ord_pos).ToString()
            Case "date", "dbtype_date", "dbtype_dbdate"
                Return reader.GetDateTime(ord_pos).ToString("yyyy-MM-dd")
            Case "uniqueidentifier"
                Return reader.GetGuid(ord_pos).ToString()
            Case Else
                Return reader.GetValue(ord_pos).ToString()
        End Select

    End Function

    Private Shared Function get_odbc_val(ByVal reader As Odbc.OdbcDataReader, ByVal ord_pos As Integer, ByVal datatype As String) As String

        If reader.IsDBNull(ord_pos) Then Return ""

        'get the type
        Select Case datatype
            Case "varchar", "nvarchar", "char", "nchar", "string", "text", "ntext",
                      "dbtype_wchar", "dbtype_wvarchar", "dbtype_wlongvarchar",
                     "dbtype_char", "dbtype_varchar", "dbtype_longvarchar"
                Return reader.GetString(ord_pos)
            Case "int", "integer", "dbtype_i4"
                Return reader.GetInt32(ord_pos).ToString
            Case "numeric", "decimal", "dbtype_numeric"
                Return reader.GetDecimal(ord_pos).ToString
            Case "datetime", "timestamp", "dbtype_dbtimestamp"
                Return reader.GetDateTime(ord_pos).ToString '("yyyy-MM-dd hh:mm:ss.fff")
            Case "float", "double"
                Return reader.GetDouble(ord_pos).ToString
            Case "bigint"
                Return reader.GetInt64(ord_pos).ToString
            Case "smallint", "dbtype_i2"
                Return reader.GetInt16(ord_pos).ToString
            Case "tinyint"
                Return reader.GetByte(ord_pos).ToString
            Case "real"
                Return reader.GetFloat(ord_pos).ToString
            Case "bit", "boolean"
                Return reader.GetBoolean(ord_pos).ToString()
            Case "date", "dbtype_date", "dbtype_dbdate"
                Return reader.GetDateTime(ord_pos).ToString("yyyy-MM-dd")
            Case "uniqueidentifier"
                Return reader.GetGuid(ord_pos).ToString()
            Case Else
                Return reader.GetValue(ord_pos).ToString()
        End Select

    End Function

    Private Class src_metadata
        Property col_nm As String
        Property is_text As Boolean
        Property ord_pos As Integer

        Property data_type As String

    End Class
End Class
