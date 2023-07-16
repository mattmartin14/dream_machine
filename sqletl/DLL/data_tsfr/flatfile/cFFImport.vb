Imports System.IO


Friend Class cFFImport

    Enum parse_ease
        happy_path = 0
        somewhat_happy_path = 1
        ahh_crap = 2
    End Enum

    Friend Shared Sub import_flatfile(file_path As String, col_delim As String, row_term As String, text_qual As String, tgt_db As String, tgt_schema As String,
                                      tgt_tbl As String, mssql_inst As String, ByVal include_headers As Boolean, ByRef err_msg As String)

        Try


            '-----------------------------------------------------------------------------------------------------------------------
            'There are a few ways this can go

            '[1] Happy path: File has a standard row terminator and if any text qualifiers, its inclosed in quotes...in that case, we use textfieldparser class
            '[2] Somewhat Happy Path: File has standard row terminator, but its text qualifier is not a double quote
            ' for this, we have to read char by char to check for quotes
            '[3] Not Happy Path: File does not have standard row terminator, thus, we read a line char by char until the row terminator, split it and load
            '       - for this situation, we have to track offsets

            Dim ease_to_parse As Integer
            Dim is_standard_row_term As Boolean = False
            Dim is_standard_text_qual As Boolean = False

            Select Case row_term
                Case vbCrLf, vbLf, vbCr, "{crlf}", "{lf}", "{cr}", "{cr}{lf}"
                    is_standard_row_term = True
            End Select

            If Trim(text_qual) = """" Or String.IsNullOrEmpty(text_qual) Then
                is_standard_text_qual = True
            End If

            If is_standard_text_qual And is_standard_row_term Then
                ease_to_parse = parse_ease.happy_path
            ElseIf is_standard_row_term Then
                ease_to_parse = parse_ease.somewhat_happy_path
            Else
                ease_to_parse = parse_ease.ahh_crap
            End If

            parse_char_by_char(file_path, col_delim, row_term, text_qual, include_headers, err_msg)

            'Select Case ease_to_parse
            '    Case parse_ease.ahh_crap
            '        parse_char_by_char(file_path, col_delim, row_term, include_headers, err_msg)
            'End Select

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub


    Private Shared Sub parse_char_by_char(file_path As String, col_delim As String, row_term As String, text_qual As String, include_headers As Boolean, ByRef err_msg As String)

        Dim dt As New DataTable
        Dim row_index As Integer = 1
        Dim row_term_length As Integer = row_term.Length
        Dim col_delim_length As Integer = col_delim.Length
        Dim text_qual_length As Integer = text_qual.Length

        Dim using_text_qual As Boolean = False
        If Not String.IsNullOrWhiteSpace(text_qual) Then
            using_text_qual = True
        End If

        Dim in_text_qual As Boolean = False

        Using reader As New StreamReader(file_path)
            Dim c As Char() = Nothing

            While Not reader.EndOfStream
                Dim row_term_buffer As String = ""
                Dim cols As New List(Of String)

                Dim curr_col As String = ""
                Do Until (row_term_buffer = row_term Or reader.EndOfStream)

                    c = New Char(0) {}
                    reader.Read(c, 0, c.Length)
                    curr_col += Convert.ToString(c)

                    If Right(curr_col, text_qual_length) = text_qual Then
                        in_text_qual = True
                    End If

                    If Right(curr_col, col_delim_length) = col_delim AndAlso Not in_text_qual Then
                        cols.Add(Left(curr_col, Len(curr_col) - col_delim_length))
                        curr_col = ""
                    End If
                    row_term_buffer = Right(curr_col, row_term_length)
                Loop

                'back off row term
                curr_col = curr_col.Replace(row_term, "")
                cols.Add(curr_col)

                If row_index = 1 Then
                    If include_headers Then
                        For Each col In cols
                            dt.Columns.Add(col, GetType(String))
                        Next
                        GoTo skip_first_row
                    Else
                        For i = 0 To cols.Count - 1
                            dt.Columns.Add("COL_" + i.ToString, GetType(String))
                        Next
                    End If
                End If

                'add the data
                Dim data_row = dt.NewRow
                For i = 0 To cols.Count - 1
                    data_row(i) = cols(i)
                Next
                dt.Rows.Add(data_row)

skip_first_row:

                row_index += 1
            End While
        End Using

        Console.WriteLine("total rows processed {0}", row_index)

        'read first 2 rows
        For i = 0 To 1
            Console.WriteLine("row: ")
            For j = 0 To dt.Columns.Count - 1
                Console.Write("[" + dt.Rows(i).Item(j).ToString + "] ")
            Next
        Next

        For Each row As DataRow In dt.Rows

        Next

    End Sub


End Class
