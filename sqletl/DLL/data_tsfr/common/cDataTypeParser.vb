
Imports System.Globalization

Friend Class cDataTypeParser

    Class src_column_metadata
        Property col_nm As String
        Property ord_pos As Integer
        Property data_type As String

        Property max_len As Integer
        Property has_mixed_types As Boolean

    End Class
    Shared Function parse_source_data_types(ByVal dt As DataTable, sample_row_cnt As Integer, ByRef err_msg As String) As List(Of src_column_metadata)

        Dim err_msg_prefix As String = "Data Type Parsing Error: "
        Try
            Dim cols As New List(Of src_column_metadata)

            'if sample rate is higher than the max number of rows
            Dim tbl_row_cnt As Integer = dt.Rows.Count
            If sample_row_cnt > tbl_row_cnt Then
                sample_row_cnt = tbl_row_cnt
            End If

            Dim ord_pos As Integer = 0
            For Each col As DataColumn In dt.Columns

                Dim src_col As New src_column_metadata With {.col_nm = col.ColumnName.ToString, .ord_pos = ord_pos}

                Dim data_type As String = ""
                Dim max_len As Integer = 0

                Dim col_has_been_initialized As Boolean = False


                'initialize the data type and length
                Dim init_val As String = Trim(dt.Rows(0)(col).ToString())
                If Not String.IsNullOrWhiteSpace(init_val) Then
                    data_type = guess_data_type(init_val)
                    max_len = init_val.Length
                    col_has_been_initialized = True
                    'set initial data type and max length
                    src_col.data_type = data_type
                    src_col.max_len = max_len
                End If

                For i As Integer = 1 To sample_row_cnt

                    Dim val As String = Trim(dt.Rows(i - 1)(col).ToString())

                    'dont evaluate nulls
                    If String.IsNullOrWhiteSpace(val) Then GoTo skip_check

                    Dim curr_data_type = guess_data_type(val)
                    Dim curr_max_len As Integer = val.Length

                    'update max length
                    If curr_max_len > src_col.max_len Then
                        src_col.max_len = curr_max_len
                    End If


                    'have the initial data types been initialized?
                    If col_has_been_initialized Then

                        'dont evaluate for mixed data types
                        If Not src_col.has_mixed_types Then

                            If data_type <> curr_data_type Then

                                'datetime vs date
                                If (data_type = "date" And curr_data_type = "datetime2") Or (data_type = "datetime2" And curr_data_type = "date") Then
                                    src_col.data_type = "datetime2"
                                    data_type = "datetime2"


                                    'tinyint, smallint, int, bigint resolve
                                ElseIf (is_some_int_type(data_type) And is_some_int_type(curr_data_type)) Then
                                    src_col.data_type = resolve_int_type(data_type, curr_data_type)
                                    data_type = src_col.data_type

                                    'varchar vs nvarchar
                                ElseIf (data_type = "varchar" And curr_data_type = "nvarchar") Or (data_type = "nvarchar" And curr_data_type = "varchar") Then
                                    src_col.data_type = "nvarchar"
                                    data_type = "nvarchar"

                                Else
                                    'we have a mixed data type
                                    src_col.has_mixed_types = True
                                    If is_unicode(val) Then
                                        src_col.data_type = "nvarchar"
                                        data_type = "nvarchar"
                                    Else
                                        src_col.data_type = "varchar"
                                        data_type = "varchar"
                                    End If
                                End If
                            End If
                        End If
                    Else
                        col_has_been_initialized = True
                        src_col.data_type = curr_data_type
                        data_type = curr_data_type
                    End If

                    val = Nothing

skip_check:
                Next


                'if we never intialized because of nulls, assume string with length of 10
                If Not col_has_been_initialized Then
                    'Console.WriteLine("column {0} was not intialized", src_col.col_nm)
                    src_col.data_type = "varchar"
                    src_col.max_len = 10
                End If

                If src_col.data_type = "date" Then
                    src_col.max_len = 10
                End If

                cols.Add(src_col)
                ord_pos += 1
            Next

            Return cols
        Catch ex As Exception
            err_msg = err_msg_prefix + ex.Message.ToString
        End Try

    End Function

    Private Shared Function is_some_int_type(data_type As String) As Boolean
        Select Case data_type
            Case "tinyint", "smallint", "int", "bigint"
                Return True
            Case Else
                Return False
        End Select
    End Function

    Private Shared Function resolve_int_type(val1 As String, val2 As String) As String

        'always choose the largest

        If val1 = "tinyint" Then
            Select Case val2
                Case "smallint"
                    Return "smallint"
                Case "int"
                    Return "int"
                Case "bigint"
                    Return "bigint"
            End Select
        ElseIf val1 = "smallint" Then
            Select Case val2
                Case "int"
                    Return "int"
                Case "bigint"
                    Return "bigint"
                Case Else
                    Return "smallint"
            End Select

        ElseIf val1 = "int" Then

            Select Case val2
                Case "bigint"
                    Return "bigint"
                Case Else
                    Return "int"
            End Select
        Else
            Return "bigint"
        End If

    End Function

    Shared Function guess_data_type(ByVal input_val As String) As String

        Dim boolval As Boolean
        Dim intval As Int32
        Dim bigintval As Int64
        Dim floatval As Double
        Dim dateval As DateTime
        Dim guidval As Guid

        Dim res As String

        If String.IsNullOrWhiteSpace(input_val) Then Return Nothing

        If Boolean.TryParse(input_val, boolval) Then
            res = "bit"
        ElseIf Int16.TryParse(input_val, intval) Then
            Dim iVal = Int16.Parse(input_val)
            If (iVal >= 0 And iVal <= 255) Then
                res = "tinyint"
            Else
                res = "smallint"
            End If
        ElseIf Int32.TryParse(input_val, intval) Then
            res = "int"
        ElseIf Int64.TryParse(input_val, bigintval) Then
            res = "bigint"
        ElseIf Double.TryParse(input_val, floatval) Then
            res = "float"
        ElseIf DateTime.TryParse(input_val, dateval) Then
            'Console.WriteLine("input val {0}", input_val)
            Dim d1 As Date
            If DateTime.TryParseExact(input_val, "HH:mm:ss.FFFFFFF", CultureInfo.InvariantCulture, DateTimeStyles.None, d1) Then
                res = "time"
            ElseIf DateTime.TryParseExact(input_val, "HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, d1) Then
                res = "time"
            Else
                Dim dt = DateTime.Parse(input_val)
                Dim hour As Integer = dt.Hour
                Dim min As Integer = dt.Minute
                Dim second As Integer = dt.Second
                If hour = 0 And min = 0 And second = 0 Then
                    res = "date"
                Else
                    res = "datetime2"
                End If
            End If

            'ElseIf Guid.TryParse(input_val, guidval) Then
            '    res = "uniqueidentifier"
        ElseIf Left(input_val, 1) = "<" AndAlso check_is_xml(input_val) Then
            res = "xml"
        ElseIf is_unicode(input_val) Then
            res = "nvarchar"
        Else
            res = "varchar"
        End If

        Return res

    End Function

    Private Shared Function is_unicode(ByVal input_val As String) As Boolean

        Dim maxAnsiCode As Integer = 255

        'for performance, if the length is greater than 1k, just return nvarchar
        If input_val.Length >= 1000 Then
            Return True
        End If

        For i As Integer = 0 To input_val.Length - 1
            If CInt(AscW(input_val(i))) > 255 Then Return True
        Next

        Return False


    End Function

    Private Shared Function check_is_xml(ByVal input_val As String) As Boolean
        Try
            'Console.WriteLine("checking if xml for {0}", input_val)
            Dim doc = XDocument.Parse(input_val)
            Return True
        Catch ex As Exception
            Return False
        End Try
    End Function


    'Friend Shared Sub test_data_type_guess(query As String, sample_rate As Integer)
    '    Dim dt As New DataTable


    '    Using cn As New SqlClient.SqlConnection("Server = MDDT\DEV16; Integrated Security = True")
    '        cn.Open()

    '        Using cmd As New SqlCommand(query, cn)
    '            Using reader = cmd.ExecuteReader

    '                For i = 0 To reader.FieldCount - 1
    '                    Dim col As New DataColumn With {.ColumnName = reader.GetName(i).ToString, .DataType = GetType(String)}
    '                    dt.Columns.Add(col)
    '                Next

    '                While reader.Read
    '                    Dim row As DataRow = dt.NewRow
    '                    For i = 0 To reader.FieldCount - 1

    '                        row(i) = reader(i).ToString
    '                    Next
    '                    dt.Rows.Add(row)
    '                End While
    '            End Using
    '        End Using
    '    End Using

    '    Dim src_meta As New List(Of src_column_metadata)

    '    src_meta = get_data_types(dt, sample_rate)

    '    For Each col In src_meta
    '        Console.WriteLine("Column Name {0}, data type [{1}], max length [{2}], ordinal position [{3}]", col.col_nm, col.data_type, col.max_len, col.ord_pos)
    '    Next


    'End Sub


End Class
