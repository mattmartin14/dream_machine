#Region "Author Notes"

'-------------------------------------------------------------------------------
'Author: Matt Martin
'Create Date: 2014-01-27
'Last Mod Date: 2017-01-14 (Added row count tsfm)

'References Needed
'[1] Microsoft.SQLServer.DTSPipeLineWrap > C:\Program Files\Microsoft SQL Server\110\SDK\Assemblies\Microsoft.SqlServer.DTSPipelineWrap
'[2] Microsoft.SqlServer.DTSRuntimeWrap > C:\Program Files\Microsoft SQL Server\110\SDK\Assemblies\Microsoft.SqlServer.DTSRuntimeWrap
'[3] add reference to microsoft.sqlserver.ManagedDTS

#End Region

Imports Microsoft.SqlServer.Dts.Runtime
Imports Microsoft.SqlServer.Dts.Runtime.Wrapper
Imports Microsoft.SqlServer.Dts.Pipeline.Wrapper
Imports System.IO
Imports System.Runtime.InteropServices

Class data_tsfr

    Private Class dft_objects

        Structure dft_dest
            Const adonet As String = "ADO Net Destination"
            Const oledb As String = "OLE DB Destination"
            Const odbc As String = "ODBC Destination"
            Const raw As String = "Raw Destination"
            Const flatfile As String = "Flat File Destination"
        End Structure

        Structure dft_source
            Const adonet As String = "ADO Net Source"
            Const oledb As String = "OLE DB Source"
            Const odbc As String = "ODBC Source"
            Const raw As String = "Raw Source"
            Const flatfile As String = "Flat File Source"
        End Structure

    End Class

    Shared data_tsfr_rows_written_msg As String

    Shared Sub data_tsfr_task(ByRef err_msg As String)

        Dim err_prefix As String = "Data Transfer Main: "
        Dim col_convert_suffix As String = "_EDISAUTOCONVERT"

        Try
            '=======================================================================================================================
            '[1] get param values

            Dim file_path As String = EDIS.get_edis_pval("file_path")



            'source vars
            Dim src_sys_nm As String = EDIS.get_edis_pval("src_sys_nm")
            Dim src_qry As String = EDIS.get_edis_pval("src_qry")

            Dim src_cn_str As String = EDIS.get_edis_pval("src_cn_str")
            Dim src_server_provider As String = EDIS.get_edis_pval("src_server_provider")
            Dim src_server_sub_provider As String = EDIS.get_edis_pval("src_server_sub_provider")


            'Dim src_cn_type As String = EDIS.get_edis_pval("src_cn_type")
            Dim src_platform As String = EDIS.get_edis_pval("src_platform")

            Dim append_to_existing_file As Boolean = CBool(EDIS.get_edis_pval("append_to_existing_file"))

            'dest vars
            Dim dest_sys_nm As String = EDIS.get_edis_pval("dest_sys_nm")
            Dim dest_tbl As String = EDIS.get_edis_pval("dest_tbl")
            Dim crt_dest_tbl As Boolean = CBool(EDIS.get_edis_pval("crt_dest_tbl"))
            Dim col_place_holder As String = EDIS.get_edis_pval("col_place_holder")
            Dim dest_cn_str As String = EDIS.get_edis_pval("dest_cn_str")

            Dim dest_server_provider As String = EDIS.get_edis_pval("dest_server_provider")
            Dim dest_server_sub_provider As String = EDIS.get_edis_pval("dest_server_sub_provider")

            'Dim dest_cn_type As String = EDIS.get_edis_pval("dest_cn_type")
            Dim dest_platform As String = EDIS.get_edis_pval("dest_platform")

            'destination load vars
            Dim load_batch_size As Integer = CInt(EDIS.get_edis_pval("load_batch_size"))
            Dim is_key_ordered As Boolean = CBool(EDIS.get_edis_pval("is_key_ordered"))
            Dim key_ordered_str As String = EDIS.get_edis_pval("key_ordered_str")
            Dim keep_ident As Boolean = CBool(EDIS.get_edis_pval("keep_ident"))
            Dim keep_nulls As Boolean = CBool(EDIS.get_edis_pval("keep_nulls"))

            Dim include_row_id As Boolean = CBool(EDIS.get_edis_pval("include_row_id_col"))

            Dim file_code_page As String = EDIS.get_edis_pval("file_code_page")
            Dim is_file_utf8 As Boolean = False
            If file_code_page.ToUpper = "UTF8" Then
                is_file_utf8 = True
            End If

            'flat file vars
            Dim load_on_ordinal As Boolean = CBool(EDIS.get_edis_pval("load_on_ordinal"))

            Dim truncate_timestamps_to_microseconds As String = EDIS.get_edis_pval("truncate_timestamps_to_microseconds")

            Dim col_delim As String = EDIS.get_edis_pval("col_delim")
            Dim row_term As String = EDIS.get_edis_pval("row_term")

            Select Case LCase(col_delim)
                Case "{cr}{lf}", "{crlf}"
                    col_delim = vbCrLf
                Case "{lf}"
                    col_delim = vbLf
                Case "{cr}"
                    col_delim = vbCr
                Case "{tab}"
                    col_delim = vbTab
                Case "{bs}"
                    col_delim = vbBack
                Case Else
                    col_delim = col_delim
            End Select


            Select Case LCase(row_term)
                Case "{cr}{lf}", "{crlf}"
                    row_term = vbCrLf
                Case "{lf}"
                    row_term = vbLf
                Case "{cr}"
                    row_term = vbCr
                Case "{tab}"
                    row_term = vbTab
                Case "{bs}"
                    col_delim = vbBack
                Case Else
                    row_term = row_term
            End Select

            'did they pass in an ascii char??
            If LCase(Left(col_delim, 7)) = "ascii::" Then
                Dim char_val_str As String = Replace(LCase(col_delim), "ascii::", "")
                Dim char_vals() As String = Split(char_val_str, ";")
                col_delim = ""
                For i As Integer = LBound(char_vals) To UBound(char_vals)
                    col_delim += Chr(char_vals(i)).ToString()
                Next
            End If

            If LCase(Left(row_term, 7)) = "ascii::" Then
                Dim char_val_str As String = Replace(LCase(row_term), "ascii::", "")
                Dim char_vals() As String = Split(char_val_str, ";")
                row_term = ""
                For i As Integer = LBound(char_vals) To UBound(char_vals)
                    row_term += Chr(char_vals(i)).ToString()
                Next
            End If




            Dim text_qual As String = EDIS.get_edis_pval("text_qual")
            Dim header_rows_to_skip As Integer = CInt(EDIS.get_edis_pval("header_rows_to_skip"))
            Dim data_rows_to_skip As Integer = CInt(EDIS.get_edis_pval("data_rows_to_skip"))
            Dim include_headers As Boolean = CBool(EDIS.get_edis_pval("include_headers"))
            Dim include_ghost_col As Boolean = CBool(EDIS.get_edis_pval("include_ghost_col"))
            Dim max_col_width As Integer = CInt(EDIS.get_edis_pval("max_col_width"))

            'extras
            Dim pkg_file_path As String = EDIS.get_edis_pval("pkg_file_path")
            Dim mssql_inst As String = EDIS.get_edis_pval("mssql_inst")

            Dim is_file_unicode As Boolean = CBool(EDIS.get_edis_pval("is_unicode"))

            log_edis_event("info", "data transfer", "parameters assigned")


            '========================================================================================================================
            '[2] Create and instantiate SSIS Package

            'create package
            Dim pkg As New Microsoft.SqlServer.Dts.Runtime.Package

            'pkg.ProtectionLevel = Microsoft.SqlServer.Dts.Runtime.DTSProtectionLevel.DontSaveSensitive
            pkg.Description = "Copyright © " + Now.Year.ToString + " MD Data Technologies, LLC. WWW.SQLETL.COM"



            'pkg.ProtectionLevel = Microsoft.SqlServer.Dts.Runtime.DTSProtectionLevel.EncryptAllWithPassword
            'pkg.PackagePassword = "xxxxxxx"

            pkg.Name = "data_tsfr"

            'set watermark
            pkg.DesignTimeProperties = dft_functions.get_pkg_annotation

            pkg.DelayValidation = False

            Dim pkg_app As New Microsoft.SqlServer.Dts.Runtime.Application

            log_edis_event("info", "application created", "done")

            '========================================================================================================================
            '[3] Add Connections for source and destination to the connection manager

            'source
            Dim src_cn As Microsoft.SqlServer.Dts.Runtime.ConnectionManager


            ' Dim ado_src_flag As Boolean = False, ado_src_sub_provider As String = Nothing



            If src_server_provider.ToUpper = "ADO.NET" Then
                src_cn = pkg.Connections.Add(cADO_Type.get_assembly_name(src_server_sub_provider.ToUpper))

                log_edis_event("info", "data transfer", "ADO.NET source connection created with sub provider (" + src_server_sub_provider.ToUpper + ")")

            ElseIf src_server_provider.ToUpper <> "RAW" Then
                src_cn = pkg.Connections.Add(src_server_provider.ToUpper)

                log_edis_event("info", "data transfer", src_server_provider.ToUpper + " source connection created")

            End If

            'If InStr(UCase(src_cn_type), "ADO.NET") > 0 Then
            '    ado_src_flag = True
            '    'get sub provider
            '    ado_src_sub_provider = Right(src_cn_type, Len(src_cn_type) - InStr(src_cn_type, "_"))
            '    src_cn_type = "ADO.NET"
            'End If

            ''if the provider is ado.net, we need to add the subcomponent source
            'If ado_src_flag = True Then
            '    Dim ado_src_cn_type As String
            '    ado_src_cn_type = cADO_Type.get_assembly_name(UCase(ado_src_sub_provider))
            '    src_cn = pkg.Connections.Add(ado_src_cn_type)
            'Else
            '    'raw files don't require connections
            '    If LCase(src_platform) <> "raw" Then
            '        src_cn = pkg.Connections.Add(src_cn_type)
            '    End If

            'End If

            'excel/access handler for connection strings
            If LCase(src_platform) = "excel" Or LCase(src_platform) = "access" Then
                src_cn_str = mCn_builder.build_cn_str(src_platform, file_path, True, include_headers)
            ElseIf LCase(src_platform) = "flatfile" Or LCase(src_platform) = "raw" Then
                src_cn_str = file_path
            End If

            'raw sources dont require connections
            If LCase(src_platform) <> "raw" Then
                src_cn.ConnectionString = src_cn_str

                'debug
                'log_edis_event("info", "data transfer", "source connection string [" + src_cn_str + "] added")
                src_cn.Name = "Source Connection"
            End If

            If UCase(src_platform) = "FLATFILE" Then

                'validate that the flat file exists
                If Not File.Exists(src_cn_str) Then
                    err_msg = "Source File  [" & src_cn_str & "] does not exist. Please ensure the file exists before invoking this package"
                    Exit Sub
                End If

                'Which type of flat file are we bringing in?
                'Fixed Width, Ragged Right, or Delimited??

                Dim fixed_width_interval_str As String = Trim(EDIS.get_edis_pval("fixed_width_intervals").ToString())
                Dim fixed_width_row_length As Integer = CInt(EDIS.get_edis_pval("fixed_width_row_len"))
                Dim fixed_width_intervals() As String
                Dim fixed_width_cols_len() As String

                Dim ragged_right_interval_str As String = Trim(EDIS.get_edis_pval("ragged_right_intervals").ToString())
                Dim ragged_right_intervals() As String
                Dim ragged_right_cols_len() As String

                Dim ff_delim_type As String = "Delimited" 'default to delimited

                Dim is_fixed_width As Boolean = False
                Dim is_ragged_right As Boolean = False

                If fixed_width_interval_str <> "" Then
                    is_fixed_width = True
                    ff_delim_type = "FixedWidth"

                    fixed_width_intervals = fixed_width_interval_str.Split(";")

                ElseIf ragged_right_interval_str <> "" Then
                    is_ragged_right = True
                    ff_delim_type = "RaggedRight"
                    ragged_right_intervals = ragged_right_interval_str.Split(";")
                End If

                'since flat file column metadata is set based on the connection manager, we need to add here
                With src_cn
                    .Properties("Format").SetValue(src_cn, ff_delim_type)
                    .Properties("ColumnNamesInFirstDataRow").SetValue(src_cn, include_headers)
                    .Properties("DataRowsToSkip").SetValue(src_cn, data_rows_to_skip)
                    .Properties("HeaderRowsToSkip").SetValue(src_cn, header_rows_to_skip)
                    .Properties("HeaderRowDelimiter").SetValue(src_cn, row_term)
                    .Properties("RowDelimiter").SetValue(src_cn, row_term)

                    If CBool(EDIS.get_edis_pval("is_unicode")) Then
                        .Properties("Unicode").SetValue(src_cn, True)
                    End If

                    'text qualifiers only apply for delimited
                    If ff_delim_type = "Delimited" Then
                        .Properties("TextQualifier").SetValue(src_cn, text_qual)
                    End If
                End With

                'open file and read first header row...run a split to get column names.....
                Dim ff_src_mgr As IDTSConnectionManagerFlatFile100 = CType(src_cn.InnerObject, IDTSConnectionManagerFlatFile100)
                Dim col_hdr_list As String = ""
                Dim sReader As New StreamReader(src_cn_str)

                'are we just reading the first line? Or did they specify to skip some header rows?
                If header_rows_to_skip = 0 Then
                    col_hdr_list = sReader.ReadLine()
                Else
                    For i As Integer = 1 To header_rows_to_skip
                        sReader.ReadLine()
                        If i = header_rows_to_skip Then
                            col_hdr_list = sReader.ReadLine()
                        End If
                    Next
                End If

                sReader.Close()

                'did the user supply a row length for flat file (Only applies for fixed width)?
                'if not, we will guess
                If fixed_width_row_length = 0 And is_fixed_width Then
                    fixed_width_row_length = col_hdr_list.Length + row_term.Length
                End If

                'add 1 for the math we are about to do on chopping the columns
                fixed_width_row_length += 1

                'if we did not get any columns error out
                If Trim(col_hdr_list) = "" Then
                    err_msg = "Could not determine column count on Source File  [" & src_cn_str & "]. Please check that you've indicated the correct number of header rows to skip."
                    Exit Sub
                End If

                'Note: Run the split through the custom split function to handle text qualifiers
                Dim cols() As String

                '-----------------------------------------------------------------------------------------
                ' {FIXED WIDTH}

                If is_fixed_width Then

                    'if the row length is not the last key, let's tack it on
                    Dim intervals_adder As Integer = 0
                    If UBound(fixed_width_intervals) <> fixed_width_row_length Then
                        intervals_adder = 1
                    End If

                    ReDim cols(UBound(fixed_width_intervals) + intervals_adder)
                    ReDim fixed_width_cols_len(UBound(fixed_width_intervals) + intervals_adder)


                    For i As Integer = LBound(fixed_width_intervals) To UBound(fixed_width_intervals) + intervals_adder

                        '------------------------------------------------------------------------------
                        'For fixed width, determine the lower and upperbounds for each split
                        Dim curr_fw_lbound As Integer, curr_fw_ubound As Integer

                        If i = 0 Then
                            'if this is the first inteval, we start at 1
                            curr_fw_lbound = 1
                        Else
                            curr_fw_lbound = curr_fw_ubound '+ 1
                        End If

                        'for upperbound, if we are on the last interval, its the row length, otherwise grab the interval
                        If i = UBound(fixed_width_intervals) + intervals_adder Then
                            curr_fw_ubound = fixed_width_row_length
                        Else
                            curr_fw_ubound = CInt(fixed_width_intervals(i))
                        End If

                        'determine the read length
                        Dim read_length As Integer = curr_fw_ubound - curr_fw_lbound

                        'update the read length array
                        fixed_width_cols_len(i) = read_length

                        'if no headers, we will asign the generic "COL_[Ordinal]"
                        If Not include_headers Then
                            cols(i) = "COL_" + i.ToString
                        Else
                            Dim curr_fw_col_nm As String

                            'if we are on the last read, just read to length of string (exclude row term +1)


                            If curr_fw_ubound = fixed_width_row_length Then

                                'String reads start at base 0, need to offset, hence the curr_fw_lbound - 1
                                curr_fw_col_nm = Trim(col_hdr_list.Substring(curr_fw_lbound - 1, read_length - row_term.Length - 1))

                            Else
                                curr_fw_col_nm = Trim(col_hdr_list.Substring(curr_fw_lbound - 1, read_length))
                            End If

                            'if it results in an empty string, then asign generic
                            If curr_fw_col_nm = "" Then
                                curr_fw_col_nm = "COL_" + i.ToString
                            End If

                            cols(i) = curr_fw_col_nm

                        End If

                    Next

                ElseIf is_ragged_right Then
                    '-----------------------------------------------------------------------------------------
                    ' {RAGGED RIGHT}
                    ' they will only supply the break points except for the last col....thus we add 1

                    ReDim cols(UBound(ragged_right_intervals) + 1)
                    ReDim ragged_right_cols_len(UBound(ragged_right_intervals) + 1)

                    'track the accumulated length for all columns prior to last one to assist when grabbing headers
                    Dim cols_accumulated_length As Integer = 0

                    For i As Integer = LBound(ragged_right_intervals) To UBound(ragged_right_intervals) + 1

                        '------------------------------------------------------------------------------
                        'determine the lower and upperbounds for each split
                        Dim curr_rr_lbound As Integer, curr_rr_ubound As Integer

                        If i = 0 Then
                            'if this is the first inteval, we start at 1
                            curr_rr_lbound = 1
                        Else
                            curr_rr_lbound = curr_rr_ubound '+ 1
                        End If

                        ' if we are on the last part of the loop, don't do this
                        If i <= UBound(ragged_right_intervals) Then
                            curr_rr_ubound = CInt(ragged_right_intervals(i))
                        End If

                        'determine the read length
                        Dim read_length As Integer

                        If i <= UBound(ragged_right_intervals) Then
                            read_length = curr_rr_ubound - curr_rr_lbound
                            cols_accumulated_length += read_length
                        Else
                            'if we are on the last iteration, set to the max col length
                            read_length = max_col_width
                        End If

                        'update the read length array
                        ragged_right_cols_len(i) = read_length

                        'if no headers, we will asign the generic "COL_[Ordinal]"
                        If Not include_headers Then
                            cols(i) = "COL_" + i.ToString
                        Else
                            Dim curr_rr_col_nm As String

                            'read the column for header purposes
                            If i <= UBound(ragged_right_intervals) Then
                                'String reads start at base 0, need to offset, hence the curr_rr_lbound - 1
                                curr_rr_col_nm = Trim(col_hdr_list.Substring(curr_rr_lbound - 1, read_length))
                            Else
                                'if we are on the last read, just read to length of string less accumulated amount
                                curr_rr_col_nm = Trim(Right(col_hdr_list, Len(col_hdr_list) - cols_accumulated_length))
                            End If

                            'if it results in an empty string, then asign generic
                            If curr_rr_col_nm = "" Then
                                curr_rr_col_nm = "COL_" + i.ToString
                            End If

                            cols(i) = curr_rr_col_nm

                        End If

                    Next


                    '-----------------------------------------------------------------------------------------
                    ' {Delimeted with Text Qualifier}
                ElseIf text_qual <> "" Then
                    'cols = custom_Split(col_hdr_list, col_delim, text_qual, True)
                    'changed on 11/3/17 to handle better splitting
                    cols = split_string(col_hdr_list, col_delim, text_qual, True).ToArray()
                Else
                    '-----------------------------------------------------------------------------------------
                    ' {Delimeted, no text qualifier}
                    'otherwise...just run a normal split
                    cols = split_string(col_hdr_list, col_delim, Nothing, True).ToArray()
                    ' cols = col_hdr_list.Split(CChar(col_delim))
                End If


                Dim cols_ubound As Integer

                'Are we adding a ghost column to the end?
                'this is needed for some csv files that come in all jacked up
                If include_ghost_col = True Then
                    cols_ubound = cols.Length
                Else
                    cols_ubound = cols.Length - 1
                End If


                '------------------------------------------------------------------------------------------------
                'Add Column Meta Data
                For i As Integer = 0 To cols_ubound

                    'add column
                    Dim ff_src_col As IDTSConnectionManagerFlatFileColumn100 = ff_src_mgr.Columns.Add()

                    'set the column name
                    Dim ff_src_col_nm As IDTSName100 = DirectCast(ff_src_col, IDTSName100)

                    'set column properties
                    With ff_src_col

                        'if we are at the end of row..set to row term (DOES NOT APPLY TO FIXED WIDTH)
                        If Not is_fixed_width Then
                            If i = cols_ubound Then
                                '.ColumnDelimiter = Environment.NewLine
                                .ColumnDelimiter = row_term ' Environment.NewLine
                            Else
                                'ragged rights don't have delimeters except for the last column
                                If Not is_ragged_right Then
                                    .ColumnDelimiter = col_delim
                                End If

                            End If
                        End If

                        'Edit 2016/02/19 to handle columns wider than 4000 char
                        'If the file is ansi...cannot use DT_NTEXT, have to use DT_TEXT....uggg

                        Dim is_ff_src_unicode As Boolean = CBool(EDIS.get_edis_pval("is_unicode"))

                        If max_col_width > 4000 And is_ff_src_unicode = False Then
                            .DataType = DataType.DT_TEXT
                        ElseIf max_col_width > 4000 And is_ff_src_unicode = True Then
                            .DataType = DataType.DT_NTEXT
                        Else
                            .DataType = DataType.DT_WSTR
                        End If


                        '.DataType = DataType.DT_WSTR

                        'for ragged right, all columns excep the last are maked as "FixedWidth"
                        ' - the last column for ragged right is marked as "Delimited"

                        If is_ragged_right Then
                            If i = cols_ubound Then
                                .ColumnType = "Delimited"
                            Else
                                .ColumnType = "FixedWidth"
                            End If
                        Else
                            .ColumnType = ff_delim_type
                        End If


                        'for fixed width, set column width and max width to the same value
                        If is_fixed_width Then
                            .ColumnWidth = CInt(fixed_width_cols_len(i))
                            .MaximumWidth = CInt(fixed_width_cols_len(i))

                            'if we are on ragged right and not at the end, set to the column length interval
                        ElseIf is_ragged_right And i < cols_ubound Then
                            .ColumnWidth = CInt(ragged_right_cols_len(i))
                            .MaximumWidth = CInt(ragged_right_cols_len(i))

                        Else
                            .MaximumWidth = max_col_width
                        End If

                        'if we put in a text qualifier, flip it to true (DOES NOT APPLY TO FIXED WIDTH)
                        If Not is_fixed_width Then
                            If text_qual <> "" Then
                                .TextQualified = True
                            Else
                                .TextQualified = False
                            End If
                        End If
                    End With

                    If include_headers = True Then
                        'if we are at the ghost column line..then we are passed the upper bound of the array
                        If (include_ghost_col = True And i = cols_ubound) Then
                            ff_src_col_nm.Name = "COL_" & i

                            'if the column name is blank, we will just give it the ordinal position
                        ElseIf Trim(cols(i).ToString()) = "" Then
                            ff_src_col_nm.Name = "COL_" & i
                        Else
                            ff_src_col_nm.Name = cols(i).ToString()
                        End If

                    Else
                        'if headers were not specified....lets just give it a "COL_" [position in array]
                        ff_src_col_nm.Name = "COL_" & i
                    End If
                Next


            End If

            '-------------------------------------------------------------------------------------------------------------
            'END SETTING SOURCE FLAT FILE
            '-------------------------------------------------------------------------------------------------------------

            log_edis_event("info", "data transfer", "source connection setup")

            '========================================================================================================================
            '[4] Create Data Flow Task: Note 


            '------------------------------------------------------------------------------
            'Add Data Flow Task 

            Dim pipe_exec As Executable = pkg.Executables.Add("STOCK:PipelineTask")

            Dim thMainPipe As Microsoft.SqlServer.Dts.Runtime.TaskHost = CType(pipe_exec, Microsoft.SqlServer.Dts.Runtime.TaskHost)
            Dim data_flow_task As MainPipe = CType(thMainPipe.InnerObject, MainPipe)
            thMainPipe.Name = "Data Flow TSFR Task"

            thMainPipe.DelayValidation = False

            'Set the Data Flow Pipe Buffer Row Size
            data_flow_task.DefaultBufferMaxRows = load_batch_size

            'added on 10/3/17
            data_flow_task.DefaultBufferSize = 1048576
            data_flow_task.RunInOptimizedMode = True

            log_edis_event("info", "data transfer", "buffer properties updated")

            If CInt(EDIS.get_edis_pval("SQL_SERVER_VERSION")) >= 130 Then
                'flip on the auto buffer adjuster and let it rip
                thMainPipe.SetExpression("AutoAdjustBufferSize", True)
                log_edis_event("info", "data transfer", "auto adjust buffer size updated")
            End If


            log_edis_event("info", "data transfer", "data flow task added")

            '----------------------------------------------------------------------------------------------------
            'ADD source and destination pre exec sql tasks

            'If we have a source pre-execute sql command, add the exec sql task
            Dim src_pre_sql_cmd_exe As Executable = Nothing

            Dim src_pre_sql_cmd As String = EDIS.get_edis_pval("src_pre_sql_cmd")
            If src_pre_sql_cmd <> "" Then
                src_pre_sql_cmd_exe = pkg.Executables.Add("STOCK:SQLTASK")

                Dim src_pre_sql_cmd_TH As Microsoft.SqlServer.Dts.Runtime.TaskHost = CType(src_pre_sql_cmd_exe, Microsoft.SqlServer.Dts.Runtime.TaskHost)

                With src_pre_sql_cmd_TH
                    .DelayValidation = True
                    .Name = "Source Pre Exec SQL Task"
                    .Properties("Connection").SetValue(src_pre_sql_cmd_TH, src_cn.ID)
                    .Properties("SqlStatementSource").SetValue(src_pre_sql_cmd_TH, src_pre_sql_cmd)
                    'cannot set it here, requires the sql statement source type which can only be brought in if i create a reference to the lib
                    'it defaults to directIntput which is what we want
                    '.Properties("SqlStatementSourceType").SetValue(tHost_pre_exec_sql_task, 0)
                    .Properties("TimeOut").SetValue(src_pre_sql_cmd_TH, CUInt(0))
                End With

                'update the source connection to persist
                src_cn.Properties("RetainSameConnection").SetValue(src_cn, "True")

                'connect the data flow to the pre exec
                pkg.PrecedenceConstraints.Add(src_pre_sql_cmd_exe, pipe_exec)

                log_edis_event("info", "data transfer", "source pre-sql command added")

            End If



            'will need to at some point capture events for the exec sql tasks

            'capture events for the data flow
            Dim build_events As cDFTBuildEventHandler = New cDFTBuildEventHandler()
            data_flow_task.Events = DtsConvert.GetExtendedInterface(TryCast(build_events, IDTSComponentEvents))

            '========================================================================================================================
            '[5] Add Data Flow Source and Destination Components


            '---------------------------------------------------
            'DFT Source

            Dim df_src_comp As IDTSComponentMetaData100 = data_flow_task.ComponentMetaDataCollection.[New]

            'Select Case UCase(src_cn_type)
            Select Case src_server_provider.ToUpper
                Case "OLEDB", "EXCEL"
                    df_src_comp.ComponentClassID = pkg_app.PipelineComponentInfos("OLE DB Source").CreationName
                    df_src_comp.Name = "Data Flow OLEDB Source"
                Case "ODBC"
                    df_src_comp.ComponentClassID = pkg_app.PipelineComponentInfos("ODBC Source").CreationName
                    df_src_comp.Name = "Data Flow ODBC Source"
                Case "ADO.NET"
                    df_src_comp.ComponentClassID = pkg_app.PipelineComponentInfos("ADO NET Source").CreationName
                    df_src_comp.Name = "Data Flow ADO Net Source"
                Case "FLATFILE"
                    df_src_comp.ComponentClassID = pkg_app.PipelineComponentInfos("Flat File Source").CreationName
                    df_src_comp.Name = "Data Flow FF Source"
                Case "RAW"
                    df_src_comp.ComponentClassID = pkg_app.PipelineComponentInfos("Raw File Source").CreationName
                    df_src_comp.Name = "Data Flow Raw Source"

            End Select

            log_edis_event("info", "data transfer", "data flow source added")

            Dim src_inst As CManagedComponentWrapper = df_src_comp.Instantiate()
            src_inst.ProvideComponentProperties()

            log_edis_event("info", "data transfer", "data flow source insantiated")

            'set the data flow source component connection
            If LCase(src_platform) <> "raw" Then
                df_src_comp.RuntimeConnectionCollection(0).ConnectionManagerID = pkg.Connections("Source Connection").ID
                df_src_comp.RuntimeConnectionCollection(0).ConnectionManager = DtsConvert.GetExtendedInterface(pkg.Connections("Source Connection"))
            End If

            log_edis_event("info", "data transfer", "data flow source connection linked")

            '-------------------------------------------------------------------------
            'Configure DFT Source
            Select Case src_server_provider.ToUpper ' UCase(src_cn_type)
                Case "OLEDB"
                    src_inst.SetComponentProperty("AccessMode", 2) '2 is sqlcommand, 1 is table name or view name, 3 is Table or View Fast
                    src_inst.SetComponentProperty("SqlCommand", src_qry)

                    Try
                        'src_inst.AcquireConnections(vbNull)
                        src_inst.AcquireConnections(Nothing)
                    Catch ex As Exception

                        err_msg = ""
                        err_msg = err_msg & "Unable to acquire connection to "
                        If LCase(src_platform) = "access" Or LCase(src_platform) = "excel" Then

                            'does the file exist (only matters for Excel)?
                            If Not File.Exists(file_path) Then
                                err_msg = src_platform.ToUpper + " source file path [" + file_path + "] does not exist. Please verify the path prior to resubmitting."
                            Else
                                err_msg = "Unable to acquire connection to source file [" + file_path + "]. " + vbCrLf
                                err_msg += "This error typically occurs for the following reasons: " + vbCrLf + vbCrLf
                                err_msg += "    1) The file is already open by the client. To resolve, close the file and rerun the transaction" + vbCrLf
                                err_msg += "    2) The ACE driver used to connect to Access and Excel is 32 bit. Try setting the @use_32_bit_runtime parameter = 1 to see if it resolves the issue."
                            End If
                        Else
                            err_msg = err_msg & "[" & src_sys_nm & "]" & vbCrLf
                        End If
                        Exit Sub

                        'dts_events.fire_error("Data Flow Source Connection Acquire (OLEDB)", err_msg)
                    End Try

                Case "ODBC"

                    'configure odbc data source
                    src_inst.SetComponentProperty("AccessMode", 1) '1 is SQL Command
                    src_inst.SetComponentProperty("SqlCommand", src_qry)
                    'NOTE: DO NOT OVERRIDE THE 1K PROPERTY ON BATCH SIZE OR IT WILL CAUSE SOME ODBC SERVERS TO HANG !!!!
                    'src_inst.SetComponentProperty("BatchSize", load_batch_size)
                    src_inst.SetComponentProperty("BatchSize", 1000)

                    'GoTo pkg_save

                    Try
                        src_inst.AcquireConnections(Nothing)
                    Catch ex As Exception
                        err_msg = "Unable to acquire connection to  [" & src_sys_nm & "] during Data Flow Source Setup. Check that the system Is accessible."
                        Exit Sub
                    End Try

                Case "ADO.NET"
                    src_inst.SetComponentProperty("AccessMode", 2) '2 is SQL Command
                    src_inst.SetComponentProperty("SqlCommand", src_qry)

                    'added 2016-07-15 to prevent timeouts on source
                    src_inst.SetComponentProperty("CommandTimeout", 0)

                    Try
                        src_inst.AcquireConnections(Nothing)
                    Catch ex As Exception
                        err_msg = "Unable to acquire connection to  [" & src_sys_nm & "] during Data Transfer Source Setup. Check that the system Is accessible."
                        Exit Sub
                    End Try
                Case "FLATFILE"
                    Try
                        src_inst.SetComponentProperty("RetainNulls", True)
                        src_inst.AcquireConnections(vbNull)
                    Catch ex As Exception
                        err_msg = "Unable to acquire connection to flatfile  [" & src_cn_str & "] during Data Flow Source Setup. Check that the file Is accessible."
                        Exit Sub
                    End Try
                Case "RAW"
                    src_inst.SetComponentProperty("AccessMode", 0) 'FileName
                    src_inst.SetComponentProperty("FileName", src_cn_str)
                    Try
                        src_inst.AcquireConnections(Nothing)
                    Catch ex As Exception
                        err_msg = "Unable to acquire connection to raw file  [" & src_cn_str & "] during Data Transfer Source Setup. Check that the file Is accessible."
                        Exit Sub
                    End Try

            End Select

            log_edis_event("info", "data transfer", "data flow source connection acquired")

            Try
                'does not trip errors on odbc...need to use second catch to see it
                src_inst.ReinitializeMetaData()

                log_edis_event("info", "data transfer", "data flow source metadata refreshed")

            Catch ex As Exception
                err_msg = dft_functions.get_dft_err_msg(src_sys_nm, file_path, src_platform)
                Exit Sub
            End Try

            src_inst.ReleaseConnections()

            'second catch for ODBC
            If df_src_comp.OutputCollection(0).OutputColumnCollection.Count = 0 Then
                err_msg = dft_functions.get_dft_err_msg(src_sys_nm, file_path, src_platform)
                Exit Sub
            End If

            '----------------------------------------------------------------------------------------------------
            '----------------------------------------------------------------------------------------------------
            'Grab Source Column Metadata

            Dim src_meta_coll As New Collection()

            For Each col As IDTSOutputColumn100 In df_src_comp.OutputCollection(0).OutputColumnCollection

                Dim src_col As New src_meta

                With src_col
                    .col_nm = col.Name
                    .data_type_str = col.DataType.ToString
                    .col_length = col.Length
                    .data_type_obj = col.DataType
                    .col_prec = col.Precision
                    .col_scale = col.Scale
                    .col_code_page_dest = col.CodePage
                End With
                src_meta_coll.Add(src_col, col.Name.ToUpper)


                'add to global dictionary
                Dim edis_dft_src_meta_col As New EDIS.c_data_transfer_src_meta
                With edis_dft_src_meta_col
                    .col_nm = col.Name
                    .dts_data_type = col.DataType.ToString
                    log_edis_event("info", "Data Transfer source metadata", "Source column [" + col.Name.ToString + "] added; data type: " + col.DataType.ToString)
                End With

                EDIS.data_transfer_src_metadata.Add(edis_dft_src_meta_col)

            Next

            'Do we just want the metadata only?
            If EDIS.get_edis_pval("get_source_qry_metadata_only") = "1" Then

                Dim src_meta_xml As String = "<SOURCE_QUERY_METADATA>" + vbCrLf
                For Each col As src_meta In src_meta_coll
                    src_meta_xml += "<COL_NAME>" + col.col_nm + "</COL_NAME>" + vbCrLf
                    src_meta_xml += "<COL_DATA_TYPE_DTS>" + col.data_type_str + "</COL_DATA_TYPE_DTS>" + vbCrLf
                    src_meta_xml += "<COL_LENGTH>" + col.col_length.ToString + "</COL_LENGTH>" + vbCrLf
                    src_meta_xml += "<COL_PRECISION>" + col.col_prec.ToString + "</COL_PRECISION>" + vbCrLf
                    src_meta_xml += "<COL_SCALE>" + col.col_scale.ToString + "</COL_SCALE>" + vbCrLf

                Next

                src_meta_xml += "</SOURCE_QUERY_METADATA>"

                Using cn As New SqlClient.SqlConnection("Server = " + EDIS._mssql_inst + "; Integrated Security = True")
                    cn.Open()

                    Using cmd As New SqlClient.SqlCommand("INSERT SSISDB.EDIS.util_task_params VALUES (@exec_id, 'SOURCE_QUERY_METADATA', @meta_xml")
                        cmd.Parameters.AddWithValue("@exec_id", EDIS._exec_id)
                        cmd.Parameters.AddWithValue("@meta_xml", src_meta_xml)
                        cmd.ExecuteNonQuery()
                    End Using
                End Using

                'get out...we are done
                pkg.Dispose()
                pkg_app = Nothing
                Exit Sub

            End If

            'does the user want use to generate the destination table on the fly??
            If (crt_dest_tbl = True) Then
                log_edis_event("info", "creating destination table", "start")


                'Need to convert old school cCRT_DEST_TBL to cDT_Tasks.create_dest_tbl
                'cDT_Tasks.create_dest_tbl(dest_sql_instance, tgt_db, tgt_schema, tgt_tbl, src_cols, False, err_msg)
                cCreateDestTbl.create_dest_tbl(src_meta_coll, err_msg)
                log_edis_event("info", "creating destination table", "done")
                If err_msg <> "" Then Exit Sub
            End If

            '====================================================================================================
            'Set Destination Connection and DFT Receiver

            Dim dest_cn As Microsoft.SqlServer.Dts.Runtime.ConnectionManager

            '----------------------------------------------------------------------------------------------------
            'For excel destinations, we need to determine the current state which is 1 of 3 options
            '   [1] The destination workbook and worksheet exist
            '   [2] The destination workbook exists but the worksheet does not
            '   [3] The destination workbook does not exist, need to use template

            Dim xl_template_temp_path As String = ""
            Dim xl_cols_list As String = ""
            Dim xl_dest_state As String = ""
            Dim does_xl_tbl_exist As Boolean = False

            If UCase(dest_sys_nm) = "EXCEL" Then

                log_edis_event("info", "Creating excel destination", "start")

                If File.Exists(file_path) Then
                    dest_cn_str = mCn_builder.build_cn_str("EXCEL", file_path, False, include_headers)

                    log_edis_event("info", "Creating excel destination", "file already exists")

                    xl_dest_state = "XL_FILE_EXISTS"
                    'Check if the target table exists, if not, we need to materialize it
                    Dim sheets() As String = cExcel_Functions.get_sheet_list(err_msg, file_path)
                    Dim sh_to_search As String = dest_tbl
                    If Right(dest_tbl, 1) <> "$" Then sh_to_search += "$"
                    For Each sh As String In sheets
                        If LCase(sh) = LCase(sh_to_search) Then
                            does_xl_tbl_exist = True
                            Exit For
                        End If
                    Next sh

                    'if the Sheet is not there, we need to create it
                    If does_xl_tbl_exist = False Then
                        cExcel_Functions.create_xl_table(dest_cn_str, dest_tbl, src_meta_coll, err_msg)
                        If err_msg <> "" Then Exit Sub
                    End If

                Else
                    xl_dest_state = "USE_TEMPLATE"

                    log_edis_event("info", "Creating excel destination", "using template")

                    'let's create the template
                    xl_template_temp_path = cExcel_Functions.get_excel_template_temp_path(err_msg)
                    If err_msg <> "" Then Exit Sub
                    cExcel_Functions.create_xl_template(xl_template_temp_path, include_headers, dest_tbl, err_msg, src_meta_coll, xl_cols_list)
                    dest_cn_str = mCn_builder.build_cn_str("EXCEL", xl_template_temp_path, False, include_headers)
                End If
            End If

            log_edis_event("info", "Excel destination creation", "done")

            '-------------------------------------------------------------------------------------------------------------------
            'Set Destinatino Connection

            'something is busted here with ADO.NET and excel....
            ' is excel not providing a sub provider?

            Select Case dest_server_provider.ToUpper ' UCase(dest_cn_type)
                'these destinations are not support (ADO.NET, EXCEL)
                Case "ADO.NET"
                    Dim ado_dest_cn_type As String = cADO_Type.get_assembly_name(dest_server_sub_provider.ToUpper)
                    dest_cn = pkg.Connections.Add(ado_dest_cn_type)
                    dest_cn.ConnectionString = dest_cn_str
                    'qa, comment this out for security
                    'log_edis_event("info", "data transfer", "destination connection string assigned [" + dest_cn_str + "]")
                    dest_cn.Name = "Destination Connection"

                    log_edis_event("info", "data transfer", "ADO.NET destination connection created")
                Case "EXCEL" ' use ADO.NET OLEDB

                    dest_cn = pkg.Connections.Add(cADO_Type.get_assembly_name("OLEDB"))
                    dest_cn.ConnectionString = dest_cn_str
                    dest_cn.Name = "Destination Connection"

                    log_edis_event("info", "data transfer", "ADO.NET destination connection created for Excel")

                    'the excel destination is horrible...do not ever use it
                'Case "EXCEL"
                '    dest_cn = pkg.Connections.Add("EXCEL")
                '    dest_cn.ConnectionString = dest_cn_str
                '    dest_cn.Name = "Destination Connection"
                Case "ACCESS"
                    dest_cn_str = mCn_builder.build_cn_str(dest_platform, xl_template_temp_path, False, include_headers)
                    dest_cn = pkg.Connections.Add(dest_server_provider.ToUpper)
                    dest_cn.ConnectionString = dest_cn_str
                    dest_cn.Name = "Destination Connection"
                Case "FLATFILE"
                    dest_cn_str = file_path
                    dest_cn = pkg.Connections.Add(dest_server_provider.ToUpper)
                    dest_cn.ConnectionString = dest_cn_str
                    dest_cn.Name = "Destination Connection"
                Case "ODBC", "OLEDB"
                    dest_cn = pkg.Connections.Add(dest_server_provider.ToUpper)
                    dest_cn.ConnectionString = dest_cn_str
                    dest_cn.Name = "Destination Connection"



                'note: for raw, we don't set a destination connection manager
                Case "RAW"
                    dest_cn_str = file_path
            End Select

            log_edis_event("info", "data transfer", "destination connection [" + dest_server_provider.ToUpper + "] setup")

            '-- -----------------------------------------------------------------------------------------------
            '-- -----------------------------------------------------------------------------------------------
            '-- Set Data Flow Destination

            Dim df_dest_comp As IDTSComponentMetaData100 = data_flow_task.ComponentMetaDataCollection.[New]

            Select Case dest_server_provider.ToUpper ' UCase(dest_cn_type)
                Case "ADO.NET", "EXCEL"
                    df_dest_comp.ComponentClassID = pkg_app.PipelineComponentInfos("ADO NET Destination").CreationName
                    df_dest_comp.Name = "Data Flow ADO NET Destination"
                'Case "EXCEL"
                '    df_dest_comp.ComponentClassID = pkg_app.PipelineComponentInfos("Excel Destination").CreationName
                '    df_dest_comp.Name = "Data Flow Excel Destination"
                Case "OLEDB"
                    df_dest_comp.ComponentClassID = pkg_app.PipelineComponentInfos("OLE DB Destination").CreationName
                    df_dest_comp.Name = "Data Flow OLEDB Destination"
                Case "ODBC"
                    df_dest_comp.ComponentClassID = pkg_app.PipelineComponentInfos("ODBC Destination").CreationName
                    df_dest_comp.Name = "Data Flow ODBC Destination"
                Case "FLATFILE"
                    df_dest_comp.ComponentClassID = pkg_app.PipelineComponentInfos("Flat File Destination").CreationName
                    df_dest_comp.Name = "Data Flow FF Destination"
                Case "RAW"
                    df_dest_comp.ComponentClassID = pkg_app.PipelineComponentInfos("Raw File Destination").CreationName
                    df_dest_comp.Name = "Data Flow Raw Destination"
            End Select

            Dim header_row_str As String = ""

            log_edis_event("info", "data transfer", "data flow destination [" + dest_server_provider.ToUpper + "] added")



            '-------------------------------------------------------------------------------------------------------------------------
            'Flat File Destination Setup

            'If the Destination is a Flat file, we need to get the file set up 
            If dest_server_provider.ToUpper = "FLATFILE" Then

                Try
                    'are we creating a new file..eg. no append?
                    If append_to_existing_file = False Then
                        'create file writer stream
                        'Dim sWriter As New IO.StreamWriter(dest_cn_str, False)

                        'are we including headers?
                        If include_headers = True Then

                            '----------------------------------------------------------
                            'assemble the header line to write

                            Dim ff_dest_col_cntr As Integer = 1

                            'loop through column meta data and add each col
                            For Each col As src_meta In src_meta_coll

                                If ff_dest_col_cntr = 1 Then
                                    header_row_str = header_row_str & col.col_nm
                                    'if we are at the last column...we add the row terminator
                                ElseIf ff_dest_col_cntr = src_meta_coll.Count Then
                                    header_row_str = header_row_str & col_delim & col.col_nm '& row_term
                                Else
                                    'add the column delimeter preceeding
                                    header_row_str = header_row_str & col_delim & col.col_nm
                                End If
                                ff_dest_col_cntr += 1
                            Next

                            'Write header line to text file
                            'EDIT: moved this to the ff data flow dest property for text qual stuff..
                            'sWriter.Write(header_row_str)

                        End If

                        'update 9/6/2017 for unicode output
                        If is_file_utf8 Then
                            IO.File.WriteAllText(dest_cn_str, header_row_str, Text.Encoding.UTF8)
                        ElseIf is_file_unicode Then
                            IO.File.WriteAllText(dest_cn_str, header_row_str, Text.Encoding.UTF8)
                        Else
                            IO.File.WriteAllText(dest_cn_str, header_row_str, Text.Encoding.ASCII)
                        End If

                        'sWriter.Close()
                        'sWriter = Nothing
                    End If

                    'set connection header properties
                    With dest_cn
                        .Properties("Format").SetValue(dest_cn, "Delimited")
                        .Properties("ColumnNamesInFirstDataRow").SetValue(dest_cn, False)
                        .Properties("DataRowsToSkip").SetValue(dest_cn, data_rows_to_skip)
                        .Properties("HeaderRowsToSkip").SetValue(dest_cn, header_rows_to_skip)
                        .Properties("HeaderRowDelimiter").SetValue(dest_cn, row_term)
                        .Properties("RowDelimiter").SetValue(dest_cn, row_term)
                        .Properties("TextQualifier").SetValue(dest_cn, text_qual)
                        If is_file_unicode Then
                            .Properties("Unicode").SetValue(dest_cn, True)
                        End If
                        If is_file_utf8 Then
                            .Properties("CodePage").SetValue(dest_cn, 65001)
                        End If
                    End With

                    'alright...lets set up the actual columns in the destination ff

                    'if for some reasons...columns already exist, lets get rid of them
                    Dim ff_dest_mgr As IDTSConnectionManagerFlatFile100 = CType(dest_cn.InnerObject, IDTSConnectionManagerFlatFile100)

                    For Each col As IDTSConnectionManagerFlatFileColumn100 In ff_dest_mgr.Columns
                        ff_dest_mgr.Columns.Remove(col)
                    Next

                    'lets add the columns and data types
                    Dim ff_dest_curr_col_cnt As Integer = 1

                    For Each col As src_meta In src_meta_coll

                        'add column
                        Dim ff_dest_col As IDTSConnectionManagerFlatFileColumn100 = ff_dest_mgr.Columns.Add()

                        'set name
                        Dim ff_dest_col_nm As IDTSName100 = DirectCast(ff_dest_col, IDTSName100)
                        ff_dest_col_nm.Name = col.col_nm



                        'set properties
                        With ff_dest_col

                            'if this is the last column...we need to specify the row term not the col term...
                            If ff_dest_curr_col_cnt = src_meta_coll.Count Then
                                .ColumnDelimiter = row_term
                            Else
                                .ColumnDelimiter = col_delim
                            End If

                            'set column data type properties

                            'if non unicode and ntext

                            .ColumnType = "Delimited"

                            'if the destination is ascii, convert unicode to ascii
                            If Not (is_file_unicode Or is_file_utf8) Then
                                Select Case col.data_type_obj
                                    Case DataType.DT_NTEXT
                                        .DataType = DataType.DT_TEXT
                                    Case DataType.DT_WSTR
                                        .DataType = DataType.DT_STR

                                    Case DataType.DT_IMAGE
                                        .DataType = DataType.DT_TEXT
                                End Select

                                'if the destination is unicode, convert ascii columns over
                            ElseIf (is_file_unicode Or is_file_utf8) Then
                                Select Case col.data_type_obj
                                    Case DataType.DT_TEXT
                                        .DataType = DataType.DT_NTEXT
                                    Case DataType.DT_STR
                                        .DataType = DataType.DT_WSTR
                                    Case DataType.DT_IMAGE
                                        .DataType = DataType.DT_NTEXT
                                End Select

                                'for unicode, convert image to dt_ntext
                            ElseIf col.data_type_obj = DataType.DT_IMAGE Then
                                .DataType = DataType.DT_NTEXT
                            Else
                                .DataType = col.data_type_obj
                            End If


                            .MaximumWidth = col.col_length
                            .DataPrecision = col.col_prec
                            .DataScale = col.col_scale



                            If text_qual <> "" Then

                                'only text qualify text...
                                Select Case LCase(col.data_type_str)
                                    Case "dt_wstr", "dt_str", "dt_ntext", "dt_text"
                                        .TextQualified = True
                                    Case Else
                                        .TextQualified = False
                                End Select

                            End If
                        End With

                        ff_dest_curr_col_cnt += 1

                    Next
                Catch ex As Exception
                    err_msg = "Unable to set up flat file destination during Data Transfer Destination Setup."
                    Exit Sub
                End Try

            End If

            '---------------------------------------------------------
            'END CREATE FLAT FILE DESTINATION
            '---------------------------------------------------------

            'instantiate the destination component
            Dim dest_inst As CManagedComponentWrapper = df_dest_comp.Instantiate()
            dest_inst.ProvideComponentProperties()

            'set the data flow destination component connection
            If dest_server_provider.ToUpper <> "RAW" Then
                df_dest_comp.RuntimeConnectionCollection(0).ConnectionManager = DtsConvert.GetExtendedInterface(pkg.Connections("Destination Connection"))
                df_dest_comp.RuntimeConnectionCollection(0).ConnectionManagerID = pkg.Connections("Destination Connection").ID
            End If

            log_edis_event("info", "data transfer", "data flow destination linked to connection")

            'set the dataflow destination comp properties
            Select Case dest_server_provider.ToUpper 'UCase(dest_cn_type)
                Case "ADO.NET", "EXCEL"
                    With dest_inst
                        .SetComponentProperty("BatchSize", load_batch_size)
                        .SetComponentProperty("UseBulkInsertWhenPossible", True)
                        .SetComponentProperty("CommandTimeout", 0)
                        If UCase(dest_server_provider.ToUpper) = "EXCEL" Then
                            'if no $ on end, append
                            Dim xl_dollar As String = IIf(Right(dest_tbl, 1) = "$", "", "$")
                            .SetComponentProperty("TableOrViewName", "`" + dest_tbl + xl_dollar + "`")
                        Else
                            .SetComponentProperty("TableOrViewName", dest_tbl)
                        End If

                    End With

                    'connect
                    Try
                        dest_inst.AcquireConnections(Nothing)
                    Catch ex As Exception
                        err_msg = "Unable to acquire connection to  [" & dest_sys_nm & "] during Data Transfer Destination Setup. Check that the system is accessible"
                        Exit Sub
                    End Try

                Case "OLEDB"
                    'If destination supports fastload, lets do it
                    If UCase(dest_platform) = "MSSQL" Then
                        dest_inst.SetComponentProperty("AccessMode", 3) 'Fast Load :> "OpenRowSet Using FastLoad"

                        '----------------------------------------------------------------
                        'set the fast load options

                        'Dim f_load_options As String = "TABLOCK, ROWS_PER_BATCH=" & load_batch_size
                        'Dim f_load_options As String = "TABLOCK, BATCHSIZE=" & load_batch_size
                        Dim f_load_options As String = "TABLOCK"
                        If is_key_ordered = True Then
                            Dim key_cols As String = mCI_Cols.get_ci_cols(mssql_inst, dest_tbl, err_msg)
                            If err_msg <> "" Then
                                Exit Sub
                            End If
                            f_load_options = f_load_options & ", ORDER(" & key_cols & ")"
                        End If
                        dest_inst.SetComponentProperty("FastLoadOptions", f_load_options)
                        dest_inst.SetComponentProperty("FastLoadKeepIdentity", keep_ident)
                        dest_inst.SetComponentProperty("FastLoadKeepNulls", keep_nulls)
                        dest_inst.SetComponentProperty("FastLoadMaxInsertCommitSize", load_batch_size)



                        'use ado.net for excel
                        'ElseIf UCase(dest_platform) = "EXCEL" Then
                        '    dest_inst.SetComponentProperty("AccessMode", 0) 'Excels access mode is 0, go figure....
                    Else
                        dest_inst.SetComponentProperty("AccessMode", 1) 'Table name ...slow row by row
                    End If

                    'set the target table
                    dest_inst.SetComponentProperty("OpenRowset", UCase(dest_tbl))

                    'connect
                    Try
                        dest_inst.AcquireConnections(vbNull)
                    Catch ex As Exception
                        err_msg = "Unable to acquire connection to  [" & dest_sys_nm & "] during Data Transfer Destination Setup. Check that the system is accessible"
                        Exit Sub
                    End Try

                Case "ODBC"

                    dest_inst.SetComponentProperty("AccessMode", 1) '1 is batch fetch
                    dest_inst.SetComponentProperty("TableName", UCase(dest_tbl))

                    '2.0 update: apply batch size for any odbc dest
                    dest_inst.SetComponentProperty("BatchSize", load_batch_size)


                    'dest_inst.SetComponentProperty("StatementTimeout", 0)
                    'dest_inst.SetComponentProperty("TransactionSize", load_batch_size)

                    Try
                        dest_inst.AcquireConnections(Nothing)
                    Catch ex As Exception
                        err_msg = "Unable to acquire connection to  [" & dest_sys_nm & "] during Data Flow Destination Setup. Check that the system is accessible"
                        Exit Sub
                    End Try

                Case "FLATFILE"
                    Try
                        If append_to_existing_file = True Then
                            dest_inst.SetComponentProperty("Overwrite", False)
                        End If

                        'if we are including headers, pop them in there
                        If include_headers = True Then
                            dest_inst.SetComponentProperty("Header", header_row_str)
                        End If

                        dest_inst.AcquireConnections(vbNull)
                    Catch ex As Exception
                        err_msg = "Unable to acquire connection to flat file path  [" & dest_cn_str & "] during Data Flow Destination Setup. Check that the file is accessible."
                        Exit Sub
                    End Try
                Case "RAW"
                    dest_inst.SetComponentProperty("AccessMode", 0) 'FileName
                    dest_inst.SetComponentProperty("FileName", dest_cn_str)
                    dest_inst.SetComponentProperty("WriteOption", 0) 'create always

                    Try
                        dest_inst.AcquireConnections(Nothing)
                    Catch ex As Exception
                        err_msg = "Unable to acquire connection to raw file  [" & dest_cn_str & "] during Data Flow Destination Setup. Check that the file is accessible."
                        Exit Sub
                    End Try
            End Select

            log_edis_event("info", "data transfer", "data flow destination connection acquired")

            'refresh metadata
            Try
                dest_inst.ReinitializeMetaData()
            Catch ex As Exception

                err_msg = ""
                err_msg += "Unable to reinitialize metadata for destination system [" & dest_sys_nm & "] "
                If dest_tbl <> "" Then err_msg += ", table [" & dest_tbl & "] "
                err_msg += "during Data Flow Destination Setup. " & vbCrLf
                err_msg += "Check your destination object and that it exists prior to re-submitting." & vbCrLf

                err_msg += dft_functions.get_dft_err_msg(dest_sys_nm, file_path, dest_platform)

                ' err_msg += "> DTS Runtime Error Readout: " & ex.ToString()
                Exit Sub

            End Try

            'if we are loading on ordinal, lets update the output column names from the source so they match the destination
            If load_on_ordinal = True Then

                'get column count
                Dim col_cnt As Integer = df_src_comp.OutputCollection(0).OutputColumnCollection.Count

                For i As Integer = 0 To col_cnt - 1
                    Dim curr_col_nm As String = df_dest_comp.InputCollection(0).ExternalMetadataColumnCollection.Item(i).Name
                    df_src_comp.OutputCollection(0).OutputColumnCollection.Item(i).Name = curr_col_nm
                Next i

                'wipe/reload
                While src_meta_coll.Count >= 1
                    src_meta_coll.Remove(1)
                End While

                For Each col As IDTSOutputColumn100 In df_src_comp.OutputCollection(0).OutputColumnCollection

                    Dim src_col As New src_meta

                    With src_col
                        .col_nm = col.Name
                        .data_type_str = col.DataType.ToString
                        .col_length = col.Length
                        .data_type_obj = col.DataType
                        .col_prec = col.Precision
                        .col_scale = col.Scale
                        .col_code_page_dest = col.CodePage
                    End With
                    src_meta_coll.Add(src_col, src_col.col_nm.ToUpper)

                Next


                'reupdate the meta data
                'src_meta_coll.Remove()

                log_edis_event("info", "data transfer", "ordinal column assignment complete")

            End If

            '------------------------------------------------------------------------------------------------------------------
            'Get the Destination Metadata so we can check if conversions are needed

            'Check if we are going to match on ordinal

            Dim need_conversion As Boolean = False

            Dim dest_meta_coll As New Collection()

            Dim ord_col_counter As Long = 0
            For Each col As IDTSExternalMetadataColumn100 In df_dest_comp.InputCollection(0).ExternalMetadataColumnCollection

                Dim dest_col As New dest_meta

                'Dim test_dest As String = col.Description

                With dest_col
                    .col_nm = col.Name
                    .data_type_str = col.DataType.ToString
                    .col_length = col.Length
                    .data_type_obj = col.DataType
                    .col_prec = col.Precision
                    .col_scale = col.Scale
                    .col_code_page = col.CodePage
                    If load_on_ordinal = True Then
                        .col_ordinal_nm = "COL_" + ord_col_counter.ToString
                    End If
                End With
                dest_meta_coll.Add(dest_col, dest_col.col_nm.ToUpper)

            Next

            'ado.net excel does not need conversion; raw destination does not need conversion
            If dest_server_provider.ToUpper = "EXCEL" Or dest_sys_nm.ToUpper = "RAW" Then GoTo skip_conversion

            'Compare Source and destination types...Do we have any unicode - non-unicode conversions going on?

            For Each col As src_meta In src_meta_coll

                '10/4 -- fixed nested loop for column comparison
                'handles mapping error here now
                If Not dest_meta_coll.Contains(col.col_nm.ToUpper) Then

                    err_msg = ""
                    err_msg += "Unable to link source query column [" & col.col_nm & "] to destination table [" & dest_tbl & "] during data flow source/destination wire-up. " & vbCrLf
                    err_msg += "Check that your source query column [" & col.col_nm & "] has a corresponding column with the same name in the destination table [" & dest_tbl & "] prior to re-submitting." & vbCrLf
                    err_msg += " -> Note: Column names are NOT case sensitive for linking together." & vbCrLf
                    Exit Sub
                End If

                Dim dcol As dest_meta = dest_meta_coll.Item(col.col_nm.ToUpper)




                'For Each dCol As dest_meta In dest_meta_coll
                'If LCase(col.col_nm) = LCase(dCol.col_nm) Then

                If (col.data_type_obj = DataType.DT_STR Or col.data_type_obj = DataType.DT_TEXT) And
                            (dCol.data_type_obj = DataType.DT_WSTR Or dCol.data_type_obj = DataType.DT_NTEXT) Then
                    If col.col_length > 4000 Then
                        col.data_type_obj_dest = DataType.DT_NTEXT
                        'col.col_length_dest = 8000
                    Else
                        col.data_type_obj_dest = DataType.DT_WSTR
                        If dcol.col_length = 0 Then
                            col.col_length_dest = col.col_length
                        Else
                            col.col_length_dest = dcol.col_length
                        End If
                    End If
                    'col.data_type_obj_dest = DataType.DT_WSTR

                    col.col_prec_dest = dcol.col_prec
                    col.col_scale_dest = dcol.col_scale
                    col.col_code_page_dest = dcol.col_code_page
                    need_conversion = True
                    col.col_needs_conversion = True
                ElseIf (col.data_type_obj = DataType.DT_WSTR Or col.data_type_obj = DataType.DT_NTEXT) And
                    (dcol.data_type_obj = DataType.DT_STR Or dcol.data_type_obj = DataType.DT_TEXT) Then

                    If col.data_type_obj = DataType.DT_WSTR Then
                        col.data_type_obj_dest = DataType.DT_STR
                    ElseIf col.data_type_obj = DataType.DT_NTEXT Then
                        col.data_type_obj_dest = DataType.DT_TEXT
                    End If

                    'col.data_type_obj_dest = DataType.DT_STR
                    col.col_length_dest = dcol.col_length
                    col.col_prec_dest = dcol.col_prec
                    col.col_scale_dest = dcol.col_scale
                    col.col_code_page_dest = dcol.col_code_page
                    need_conversion = True
                    col.col_needs_conversion = True

                    'for dt image converions
                ElseIf col.data_type_obj = DataType.DT_IMAGE And
                    (dcol.data_type_obj = DataType.DT_NTEXT Or dcol.data_type_obj = DataType.DT_TEXT) Then


                    If is_file_unicode Then
                        col.data_type_obj_dest = DataType.DT_NTEXT
                    Else
                        col.data_type_obj_dest = DataType.DT_TEXT
                        col.col_code_page_dest = dcol.col_code_page
                    End If


                    need_conversion = True
                    col.col_needs_conversion = True

                ElseIf (col.data_type_obj = DataType.DT_DBTIMESTAMP2 Or col.data_type_obj = DataType.DT_DBTIMESTAMP) And truncate_timestamps_to_microseconds = "1" Then
                    col.col_scale_dest = 6
                    need_conversion = True
                    col.col_needs_conversion = True
                    col.data_type_obj_dest = DataType.DT_DBTIMESTAMP2

                ElseIf col.data_type_obj = DataType.DT_DBTIMESTAMPOFFSET And truncate_timestamps_to_microseconds = "1" Then
                    col.col_scale_dest = 6
                    need_conversion = True
                    col.col_needs_conversion = True
                    col.data_type_obj_dest = DataType.DT_DBTIMESTAMPOFFSET

                End If

                'Exit For
                'End If
                'Next

            Next

            Dim df_convert_task As IDTSComponentMetaData100

            'If we need a conversion, lets add it
            If need_conversion = True Then



                df_convert_task = data_flow_task.ComponentMetaDataCollection.[New]()

                df_convert_task.ComponentClassID = pkg_app.PipelineComponentInfos("Data Conversion").CreationName
                df_convert_task.Name = "Data Conversion"
                Dim df_con_wrapper As CManagedComponentWrapper = df_convert_task.Instantiate()
                df_con_wrapper.ProvideComponentProperties()

                log_edis_event("info", "data transfer", "data conversion task created")

                'attach source to data conversion
                data_flow_task.PathCollection.[New]().AttachPathAndPropagateNotifications(df_src_comp.OutputCollection(0), df_convert_task.InputCollection(0))

                Dim mapped_col_id As Integer = 5000

                'Loop through source columns and convert
                For Each col As src_meta In src_meta_coll
                    If col.col_needs_conversion = True Then

                        log_edis_event("info", "data transfer", "setting conversion for column" + col.col_nm)

                        Dim df_con_virt_input As IDTSVirtualInput100 = df_convert_task.InputCollection(0).GetVirtualInput()
                        Dim df_con_output As IDTSOutput100 = df_convert_task.OutputCollection(0)
                        Dim df_con_output_cols As IDTSOutputColumnCollection100 = df_con_output.OutputColumnCollection

                        'get the lineage id
                        Dim src_lineage_id As Integer = df_con_virt_input.VirtualInputColumnCollection(col.col_nm).LineageID

                        'set readonly...we will make a copy outbound
                        df_con_wrapper.SetUsageType(df_convert_task.InputCollection(0).ID, df_con_virt_input, src_lineage_id, DTSUsageType.UT_READONLY)

                        'create the new output column
                        Dim df_con_out_col As IDTSOutputColumn100 = df_con_wrapper.InsertOutputColumnAt(df_con_output.ID, 0, col.col_nm, String.Empty)

                        'set thew new output column type
                        Dim code_page As Integer
                        If col.data_type_obj_dest = DataType.DT_STR Then
                            'code_page = 1252
                            code_page = col.col_code_page_dest
                        ElseIf col.data_type_obj_dest = DataType.DT_TEXT Then
                            code_page = col.col_code_page_dest
                        Else
                            code_page = 0
                        End If

                        'set the new data type



                        Dim con_log_msg As String =
                            String.Format("Setting Data conversion for column [" + col.col_nm + "] for data type {0}, length {1}, precision {2}, scale {3}, code page {4}", col.data_type_obj_dest, col.col_length_dest, col.col_prec_dest, col.col_scale_dest, code_page)

                        log_edis_event("info", "data transfer conversion", con_log_msg)

                        df_con_out_col.SetDataTypeProperties(col.data_type_obj_dest, col.col_length_dest, col.col_prec_dest, col.col_scale_dest, code_page)

                        log_edis_event("info", "data transfer", "data conversion set for column " + col.col_nm)

                        If (col.data_type_obj = DataType.DT_DBTIMESTAMP2 Or col.data_type_obj = DataType.DT_DBTIMESTAMP Or col.data_type_obj = DataType.DT_DBTIMESTAMPOFFSET) And truncate_timestamps_to_microseconds = "1" Then
                            df_con_out_col.TruncationRowDisposition = DTSRowDisposition.RD_IgnoreFailure
                            df_con_out_col.ErrorOrTruncationOperation = "" 'leaving it blank tells it to ignore
                        End If

                        'set the name with a conversion signal so we know which one to grab on the wireup
                        'df_con_out_col.Name = col.col_nm + "_conv"
                        df_con_out_col.Name = col.col_nm + col_convert_suffix

                        'set the new output id
                        df_con_out_col.MappedColumnID = mapped_col_id
                        'increment the new column mapping id placeholder
                        mapped_col_id += 1

                        'link source input to destination output
                        df_con_wrapper.SetOutputColumnProperty(df_con_output.ID, df_con_out_col.ID, "SourceInputColumnLineageID", src_lineage_id)

                    End If
                Next

                log_edis_event("info", "data transfer", "data conversion columns linked")
            End If



skip_conversion:

            Dim src_output As IDTSOutput100

            'if we had a conversion, the input is then the conversion task

            If need_conversion = True Then
                src_output = df_convert_task.OutputCollection(0)
                'ElseIf include_row_id = True Then
                '    src_output = row_id_script_tsfm_CMDT.OutputCollection(0)
            Else
                src_output = df_src_comp.OutputCollection(0)
            End If

            '-----------------------------------------------------------------------------------------------------------------------------------
            'Add Row Count Monitor

            log_edis_event("info", "add row counter", "start")

            pkg.Variables.Add("row_cnt", False, "User", 0)

            Dim row_cnt_tsfm As IDTSComponentMetaData100 = data_flow_task.ComponentMetaDataCollection.[New]
            'dont add components this way...always use the pipeline component infos
            'row_cnt_tsfm.ComponentClassID = "DTSTransform.RowCount"
            row_cnt_tsfm.ComponentClassID = pkg_app.PipelineComponentInfos("Row Count").CreationName
            row_cnt_tsfm.Name = "Row Counter"

            Dim row_cnt_instance As CManagedComponentWrapper = row_cnt_tsfm.Instantiate()
            row_cnt_instance.ProvideComponentProperties()
            row_cnt_instance.SetComponentProperty("VariableName", "User::row_cnt")

            log_edis_event("info", "add row counter", "added")

            'attached prior task to row count
            data_flow_task.PathCollection.[New].AttachPathAndPropagateNotifications(src_output, row_cnt_tsfm.InputCollection(0))

            log_edis_event("info", "add row counter", "attached from source")

            'attach row count to destination
            Dim dest_input As IDTSInput100 = df_dest_comp.InputCollection(0)

            data_flow_task.PathCollection.[New].AttachPathAndPropagateNotifications(row_cnt_tsfm.OutputCollection(0), dest_input)

            log_edis_event("info", "add row counter", "attached to destination")

            '-----------------------------------------------------------------------------------------------------------------------------------------
            'Wire Up Source and Destination

            Dim vdest_input As IDTSVirtualInput100 = dest_input.GetVirtualInput()

            'Dim curr_input_col_name As String

            Dim upStreamInputVirtColCollection As New Dictionary(Of String, Integer)

            'map upstream columns to the destination
            ' upstream columns are known in DTS land as "VirtualInputColumn"
            For Each upStreamInputVirtCol As IDTSVirtualInputColumn100 In vdest_input.VirtualInputColumnCollection

                Dim needs_conversion As Boolean = False

                'only map columns that didn't require a conversion
                If src_meta_coll.Contains(upStreamInputVirtCol.Name.ToUpper) Then
                    needs_conversion = src_meta_coll.Item(upStreamInputVirtCol.Name.ToUpper).col_needs_conversion
                End If


                If Not needs_conversion Then
                    Dim destInputCol As IDTSInputColumn100 = dest_inst.SetUsageType(dest_input.ID, vdest_input, upStreamInputVirtCol.LineageID, DTSUsageType.UT_READONLY)
                    'add to the dictionary so we can lookup later (uppercase it so its a quicker search)
                    upStreamInputVirtColCollection.Add(destInputCol.Name.ToUpper, destInputCol.ID)

                    log_edis_event("info", "data flow task", String.Format("Destination Input Column {0}, ID {1} loaded to list", destInputCol.Name.ToUpper, destInputCol.ID))
                End If


            Next

            log_edis_event("info", "data flow task", "data flow destination input columns created")

            '----------------------------------------------------------------------------------------------------------
            '***** RAW FILES DO NOT NEED TO BE MATCHED ON METADATA AND DESTIONATION INPUTS..so we skip

            If dest_sys_nm.ToUpper = "RAW" Then GoTo skip_metadata_match

            '----------------------------------------------------------------------------------------------------------
            'Load the External Metadata columns for the destination

            Dim destMetadataColCollection As New Dictionary(Of String, Integer)

            For Each col As IDTSExternalMetadataColumn100 In dest_input.ExternalMetadataColumnCollection
                destMetadataColCollection.Add(col.Name.ToUpper, col.ID)
                log_edis_event("info", "data flow task", String.Format("Destination Metadata Column {0}, ID {1} loaded to list", col.Name.ToUpper, col.ID))
            Next

            log_edis_event("info", "data flow task", "data flow destination external column collection loaded")

            '--------------------------------------------------------------------------------------
            'link the destination input columns to the corresponding metadata columns
            'Special Note on Conversions: If the source has a conversion, we don't want to map it, we want to map the converted version

            For Each destInputCol In upStreamInputVirtColCollection

                Dim target_src_col As String = destInputCol.Key

                'strip out the suffix for converted columns
                If Right(destInputCol.Key, Len(col_convert_suffix)) = col_convert_suffix Then
                    'If Right(destInputCol.Key, 5) = "_CONV" Then
                    'target_src_col = Left(target_src_col, Len(target_src_col) - 5)
                    target_src_col = Left(target_src_col, Len(target_src_col) - Len(col_convert_suffix))
                End If

                'get the destination metadata column (what the user sees as the name)
                Dim dest_meta_input_id As Integer = destMetadataColCollection.Item(target_src_col)

                'pair the destination input column to the external metadata column
                dest_inst.MapInputColumn(dest_input.ID, destInputCol.Value, dest_meta_input_id)

            Next


skip_metadata_match:

            log_edis_event("info", "data transfer", "data flow origin/destination columns linked")


            If need_conversion Then
                dest_inst.ReinitializeMetaData()
            End If

            dest_inst.ReleaseConnections()
            dest_inst = Nothing

            '------------------------------------------------------------------------------------------------------------------------
            'Add SQL Task to add row count

            If Not EDIS.skip_data_transfer_log_row Then

                'Add OLEDB connection for Host SQL Server
                Dim host_cn_str As String = String.Format("Provider=SQLOLEDB;Data Source={0};Initial Catalog={1};Integrated Security=SSPI;", EDIS._mssql_inst, "SSISDB")
                Dim host_cn = cDTSTassks.add_cn(pkg, "OLEDB", host_cn_str, "EDIS Host MSSQL Instance")

                Dim row_cnt_exec_sql_task As Executable = pkg.Executables.Add("STOCK:SQLTASK")

                Dim row_cnt_exec_sql_task_TH As Microsoft.SqlServer.Dts.Runtime.TaskHost = CType(row_cnt_exec_sql_task, Microsoft.SqlServer.Dts.Runtime.TaskHost)

                Dim row_update_cmd As String =
                    """EXEC EDIS.isp_log_info @exec_id = '" + EDIS._exec_id + "', @action = 'rows_tsfr', @val = '"" + (DT_WSTR, 100) @[User::row_cnt] +""'"""

                With row_cnt_exec_sql_task_TH
                    .DelayValidation = True
                    .Name = "Log Rows"
                    'since the connection is not part of the data flow, its having some issue resolving it, so we add as an expression instead
                    '.Properties("Connection").SetValue(row_cnt_exec_sql_task_TH, host_cn.ID)
                    .SetExpression("Connection", """" + host_cn.Name + """")
                    .SetExpression("SqlStatementSource", row_update_cmd)
                    .Properties("TimeOut").SetValue(row_cnt_exec_sql_task_TH, CUInt(0))
                End With

                'connect the data flow to the pre exec
                pkg.PrecedenceConstraints.Add(pipe_exec, row_cnt_exec_sql_task)


                log_edis_event("info", "data transfer", "row count exec sql task added")
            End If

            '--------------------------------------------------------------------------------------------------------------
            '[6] Execute the package

            'pkg_save:

            If pkg_file_path <> "" Then
                'If save_pkg = True Then
                pkg_app.SaveToXml(pkg_file_path, pkg, Nothing)
            End If



            Dim ev As New package_event_listener()

            Dim pkgResults As Microsoft.SqlServer.Dts.Runtime.DTSExecResult

            log_edis_event("info", "data transfer", "starting package execution")

            EDIS.info_msgs.Append(String.Format("<DATA_TRANSFER_EXEC_START_TS>{0}</DATA_TRANSFER_EXEC_START_TS>", EDIS.get_now_as_string))

            'run it
            pkgResults = pkg.Execute(Nothing, Nothing, ev, Nothing, Nothing)

            EDIS.info_msgs.Append(String.Format("<DATA_TRANSFER_EXEC_END_TS>{0}</DATA_TRANSFER_EXEC_END_TS>", EDIS.get_now_as_string))

            log_edis_event("info", "data transfer", "package execution complete")

            If pkgResults = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure Then
                log_edis_event("error", "data transfer", "gathering error messages")
                For Each Err As DtsError In pkg.Errors
                    err_msg += Err.Description.ToString
                Next
                Exit Sub
            End If

            'If the destination is excel, we are not done yet, need to send results to final sheet
            If UCase(dest_platform) = "EXCEL" And xl_dest_state = "USE_TEMPLATE" Then

                cExcel_Functions.send_xl_sheet_to_final_path(err_msg, xl_template_temp_path, False, xl_cols_list)

                'Thread.Sleep(3000)
                Dim dl_cnt As Integer = 0
                Try
try_to_delete_again:

                    'drop the temp sheet
                    If File.Exists(xl_template_temp_path) Then
                        File.Delete(xl_template_temp_path)
                    End If
                Catch ex As System.IO.IOException
                    Exit Sub
                    'Thread.Sleep(3000)
                    'dl_cnt += 1
                    'If dl_cnt >= 4 Then
                    '    err_msg = ""
                    '    Exit Sub
                    'End If
                    'GoTo try_to_delete_again
                End Try
            End If

            Exit Sub

        Catch exC As COMException

            Dim err_symbol_nm As String = cHResLookup.get_com_symbolic_err_msg(exC.ErrorCode)

            err_msg = err_prefix + String.Format("COM Exception {0}; Error {1}", exC.HResult.ToString, err_symbol_nm)

            Exit Sub

        Catch ex As Exception

            Dim sys_err_msg As String = ex.Message.ToString

            err_msg = err_prefix + ex.Message.ToString()

            If Main.show_call_stack Then
                err_msg += err_msg + ex.StackTrace.ToString()
            End If


        End Try

    End Sub

    ''handles event at run time of ssis package execution
    Class package_event_listener

        Inherits DefaultEvents

        'Public dts As Microsoft.SqlServer.Dts.Tasks.ScriptTask.ScriptObjectModel

        'Public Sub New(dts As Microsoft.SqlServer.Dts.Tasks.ScriptTask.ScriptObjectModel)
        '    Me.dts = dts
        'End Sub

        Public Overrides Function OnError(source As DtsObject, errorCode As Integer, subComponent As String, description As String, helpFile As String, helpContext As Integer,
            idofInterfaceWithError As String) As Boolean

            'Console.WriteLine("Error in {0}/{1} : {2}", source, subComponent, description)
            'dts.Events.FireError(errorCode, subComponent, description, helpFile, helpContext)
            ' Continue executing the task even though an error was triggered.
            Return False
        End Function

        Public Overrides Sub OnInformation(source As DtsObject, informationCode As Integer, subComponent As String, description As String, helpFile As String, helpContext As Integer,
         idofInterfaceWithError As String, ByRef fireAgain As Boolean)

            If Not String.IsNullOrEmpty(description) Then

                Dim msg As String = description.ToString().Replace(vbCrLf, "")

                EDIS.info_msgs.Append(String.Format("<DTS_EXEC_INFO>{0} - {1}</DTS_EXEC_INFO>", EDIS.get_now_as_string, msg))

                'log_edis_event("dts_exec", "onInfo", Left(description.ToString, 4000))

            End If

            'log the rows transferred
            'If description Like "*wrote*rows*" Then
            '    data_tsfr_rows_written_msg = description
            'End If

            'dts.Events.FireInformation(informationCode, subComponent, description, helpFile, helpContext, fireAgain)
        End Sub

        'Public Overrides Sub OnWarning(source As DtsObject, warningCode As Integer, subComponent As String, description As String, helpFile As String, helpContext As Integer,
        ' idofInterfaceWithError As String)
        '    'dts.Events.FireWarning(warningCode, subComponent, description, helpFile, helpContext)
        'End Sub

    End Class

    'Class to hold source meta data column information
    Class src_meta

        Property col_needs_conversion As Boolean
        Property col_nm As String
        Property data_type_str As String
        Property col_length As Integer
        Property col_length_dest As Integer
        Property col_prec As Integer
        Property col_prec_dest As Integer
        Property col_scale As Integer
        Property col_scale_dest As Integer
        Property col_code_page_dest As Integer
        Property data_type_obj As DataType
        Property data_type_obj_dest As DataType

    End Class

    'Class to hold destination meta data column information
    Class dest_meta

        Property col_ordinal_nm As String
        Property col_nm As String
        Property data_type_str As String
        Property col_length As Integer
        Property col_prec As Integer
        Property col_scale As Integer
        Property col_code_page As Integer
        Property data_type_obj As DataType


    End Class



End Class
