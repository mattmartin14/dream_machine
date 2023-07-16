
'Imports Microsoft.SqlServer.Dts.Runtime
'Imports Microsoft.SqlServer.Dts.Runtime.Wrapper
'Imports Microsoft.SqlServer.Dts.Pipeline.Wrapper

'Class cBuildTestXLPackage

'    Shared Sub build_xl_pkg()

'        Dim pkg As New Microsoft.SqlServer.Dts.Runtime.Package
'        Dim app As New Microsoft.SqlServer.Dts.Runtime.Application

'        '---------------------------------------------------------------
'        'set connections

'        Dim src_cn As Microsoft.SqlServer.Dts.Runtime.ConnectionManager
'        src_cn = pkg.Connections.Add("OLEDB")
'        src_cn.Name = "Source Connection"
'        src_cn.ConnectionString = "Provider=SQLOLEDB; Data Source = localhost; Integrated Security = SSPI"

'        Dim dest_cn As Microsoft.SqlServer.Dts.Runtime.ConnectionManager
'        dest_cn = pkg.Connections.Add("EXCEL")
'        dest_cn.Name = "Destination Connection"
'        dest_cn.ConnectionString = "Provider=Microsoft.ACE.OLEDB.12.0; Data Source = C:\Temp\Book1.xlsx; Extended Properties = ""Excel 12.0 XML; HDR=YES;IMEX=0"""

'        '---------------------------------------------------------------
'        'add dft task
'        Dim pipe_exec As Executable = pkg.Executables.Add("STOCK:PipelineTask")

'        Dim thMainPipe As Microsoft.SqlServer.Dts.Runtime.TaskHost = CType(pipe_exec, Microsoft.SqlServer.Dts.Runtime.TaskHost)
'        Dim data_flow_task As MainPipe = CType(thMainPipe.InnerObject, MainPipe)
'        thMainPipe.Name = "Data Flow TSFR Task"

'        thMainPipe.DelayValidation = False


'        '---------------------------------------------------------------
'        'set dft source

'        Dim df_src_comp As IDTSComponentMetaData100 = data_flow_task.ComponentMetaDataCollection.[New]
'        df_src_comp.ComponentClassID = app.PipelineComponentInfos("OLE DB Source").CreationName
'        df_src_comp.Name = "Data Flow OLEDB Source"

'        Dim src_inst As CManagedComponentWrapper = df_src_comp.Instantiate()
'        src_inst.ProvideComponentProperties()
'        df_src_comp.RuntimeConnectionCollection(0).ConnectionManagerID = pkg.Connections("Source Connection").ID
'        df_src_comp.RuntimeConnectionCollection(0).ConnectionManager = DtsConvert.GetExtendedInterface(pkg.Connections("Source Connection"))

'        Dim src_qry As String = "SELECT loc_id, loc_nm, loc_desc from sandbox_test.dbo.xl_data"

'        src_inst.SetComponentProperty("AccessMode", 2)
'        src_inst.SetComponentProperty("SqlCommand", src_qry)

'        src_inst.AcquireConnections(Nothing)
'        src_inst.ReinitializeMetaData()
'        src_inst.ReleaseConnections()

'        Dim src_meta_coll As New Collection()

'        For Each col As IDTSOutputColumn100 In df_src_comp.OutputCollection(0).OutputColumnCollection

'            Dim src_col As New src_meta

'            With src_col
'                .col_nm = col.Name
'                .data_type_str = col.DataType.ToString
'                .col_length = col.Length
'                .data_type_obj = col.DataType
'                .col_prec = col.Precision
'                .col_scale = col.Scale
'                .col_code_page_dest = col.CodePage
'            End With
'            src_meta_coll.Add(src_col)

'        Next


'        '---------------------------------------------------------------
'        'set dft dest

'        Dim df_dest_comp As IDTSComponentMetaData100 = data_flow_task.ComponentMetaDataCollection.[New]
'        df_dest_comp.ComponentClassID = app.PipelineComponentInfos("Excel Destination").CreationName
'        df_dest_comp.Name = "Data Flow Excel Destination"
'        Dim dest_inst As CManagedComponentWrapper = df_dest_comp.Instantiate()
'        dest_inst.ProvideComponentProperties()
'        df_dest_comp.RuntimeConnectionCollection(0).ConnectionManager = DtsConvert.GetExtendedInterface(pkg.Connections("Destination Connection"))
'        df_dest_comp.RuntimeConnectionCollection(0).ConnectionManagerID = pkg.Connections("Destination Connection").ID

'        With dest_inst
'            .SetComponentProperty("AccessMode", 0)
'            .SetComponentProperty("OpenRowset", "Sheet1$")
'        End With
'        dest_inst.AcquireConnections(vbNull)
'        dest_inst.ReinitializeMetaData()

'        '==================================================================================================================
'        '==================================================================================================================
'        'add conversion

'        Dim need_conversion As Boolean = False

'        Dim dest_meta_coll As New Collection()

'        For Each col As IDTSExternalMetadataColumn100 In df_dest_comp.InputCollection(0).ExternalMetadataColumnCollection

'            Dim dest_col As New dest_meta

'            With dest_col
'                .col_nm = col.Name
'                .data_type_str = col.DataType.ToString
'                .col_length = col.Length
'                .data_type_obj = col.DataType
'                .col_prec = col.Precision
'                .col_scale = col.Scale
'                .col_code_page = col.CodePage
'            End With
'            dest_meta_coll.Add(dest_col)

'        Next

'        For Each col As src_meta In src_meta_coll
'            For Each dCol As dest_meta In dest_meta_coll
'                If LCase(col.col_nm) = LCase(dCol.col_nm) Then

'                    If (col.data_type_obj = DataType.DT_STR Or col.data_type_obj = DataType.DT_TEXT) And _
'                        (dCol.data_type_obj = DataType.DT_WSTR Or dCol.data_type_obj = DataType.DT_NTEXT) Then
'                        'If UCase(dest_platform) = "EXCEL" Then
'                        '    col.data_type_obj_dest = DataType.DT_NTEXT
'                        If col.col_length > 4000 Then
'                            'if the source varchar is greater than 4K, need to go to next
'                            col.data_type_obj_dest = DataType.DT_NTEXT
'                            col.col_length_dest = 8000
'                        Else
'                            col.data_type_obj_dest = DataType.DT_WSTR
'                            If dCol.col_length = 0 Then
'                                col.col_length_dest = col.col_length
'                            Else
'                                col.col_length_dest = dCol.col_length
'                            End If
'                        End If
'                        'col.data_type_obj_dest = DataType.DT_WSTR

'                        col.col_prec_dest = dCol.col_prec
'                        col.col_scale_dest = dCol.col_scale
'                        col.col_code_page_dest = dCol.col_code_page
'                        need_conversion = True
'                        col.col_needs_conversion = True
'                    ElseIf (col.data_type_obj = DataType.DT_WSTR Or col.data_type_obj = DataType.DT_NTEXT) And _
'                        (dCol.data_type_obj = DataType.DT_STR Or dCol.data_type_obj = DataType.DT_TEXT) Then

'                        If col.data_type_obj = DataType.DT_WSTR Then
'                            col.data_type_obj_dest = DataType.DT_STR
'                        ElseIf col.data_type_obj = DataType.DT_NTEXT Then
'                            col.data_type_obj_dest = DataType.DT_TEXT
'                        End If

'                        'col.data_type_obj_dest = DataType.DT_STR
'                        col.col_length_dest = dCol.col_length
'                        col.col_prec_dest = dCol.col_prec
'                        col.col_scale_dest = dCol.col_scale
'                        col.col_code_page_dest = dCol.col_code_page
'                        need_conversion = True
'                        col.col_needs_conversion = True
'                    End If

'                    Exit For
'                End If
'            Next

'        Next

'        Dim df_convert_task As IDTSComponentMetaData100



'        'If we need a conversion, lets add it
'        If need_conversion = True Then

'            df_convert_task = data_flow_task.ComponentMetaDataCollection.[New]()

'            df_convert_task.ComponentClassID = "DTSTransform.DataConvert"
'            df_convert_task.Name = "Data Conversion"
'            Dim df_con_wrapper As CManagedComponentWrapper = df_convert_task.Instantiate()
'            df_con_wrapper.ProvideComponentProperties()

'            'attach source to data conversion
'            data_flow_task.PathCollection.[New]().AttachPathAndPropagateNotifications(df_src_comp.OutputCollection(0), df_convert_task.InputCollection(0))

'            Dim mapped_col_id As Integer = 5000

'            'Loop through source columns and convert
'            For Each col As src_meta In src_meta_coll
'                If col.col_needs_conversion = True Then

'                    Dim df_con_virt_input As IDTSVirtualInput100 = df_convert_task.InputCollection(0).GetVirtualInput()
'                    Dim df_con_output As IDTSOutput100 = df_convert_task.OutputCollection(0)
'                    Dim df_con_output_cols As IDTSOutputColumnCollection100 = df_con_output.OutputColumnCollection

'                    'get the lineage id
'                    Dim src_lineage_id As Integer = df_con_virt_input.VirtualInputColumnCollection(col.col_nm).LineageID

'                    'set readonly...we will make a copy outbound
'                    df_con_wrapper.SetUsageType(df_convert_task.InputCollection(0).ID, df_con_virt_input, src_lineage_id, DTSUsageType.UT_READONLY)

'                    'create the new output column
'                    Dim df_con_out_col As IDTSOutputColumn100 = df_con_wrapper.InsertOutputColumnAt(df_con_output.ID, 0, col.col_nm, String.Empty)

'                    'set thew new output column type
'                    Dim code_page As Integer
'                    If col.data_type_obj_dest = DataType.DT_STR Then
'                        'code_page = 1252
'                        code_page = col.col_code_page_dest
'                    Else
'                        code_page = 0
'                    End If

'                    'set the new data type
'                    df_con_out_col.SetDataTypeProperties(col.data_type_obj_dest, col.col_length_dest, col.col_prec_dest, col.col_scale_dest, code_page)

'                    'set the name with a conversion signal so we know which one to grab on the wireup
'                    df_con_out_col.Name = col.col_nm + "_conv"

'                    'set the new output id
'                    df_con_out_col.MappedColumnID = mapped_col_id
'                    'increment the new column mapping id placeholder
'                    mapped_col_id += 1

'                    'link source input to destination output
'                    df_con_wrapper.SetOutputColumnProperty(df_con_output.ID, df_con_out_col.ID, "SourceInputColumnLineageID", src_lineage_id)

'                End If
'            Next


'        End If

'        '---------------------------------------------------------------
'        'wire up

'        Dim src_output As IDTSOutput100

'        'if we had a conversion, the input is then the conversion task
'        If need_conversion = True Then
'            src_output = df_convert_task.OutputCollection(0)
'        Else
'            src_output = df_src_comp.OutputCollection(0)
'        End If

'        Dim dest_input As IDTSInput100 = df_dest_comp.InputCollection(0)

'        '
'        'dest_input.ErrorRowDisposition = DTSRowDisposition.RD_RedirectRow

'        Dim df_path As IDTSPath100 = data_flow_task.PathCollection.[New]
'        df_path.AttachPathAndPropagateNotifications(src_output, dest_input)

'        Dim vdest_input As IDTSVirtualInput100 = dest_input.GetVirtualInput()

'        Dim curr_input_col_name As String

'        For Each vColumn As IDTSVirtualInputColumn100 In vdest_input.VirtualInputColumnCollection

'            curr_input_col_name = vColumn.Name

'            'map the column
'            Dim vCol As IDTSInputColumn100 = dest_inst.SetUsageType(dest_input.ID, vdest_input, vColumn.LineageID, DTSUsageType.UT_READONLY)

'            'since the destination column can be upper/lowercase and we don't have control over it, we need to compare to the virtual column name and get the id
'            Dim curr_destx_col_id As Integer

'            curr_destx_col_id = Nothing

'            'was this column converted? if so...we need to ignore the original version
'            For Each col As src_meta In src_meta_coll
'                If LCase(col.col_nm) = LCase(vColumn.Name) Then
'                    If col.col_needs_conversion = True Then
'                        GoTo skip_wireup
'                    Else
'                        Exit For
'                    End If
'                End If
'            Next

'            'loop through to see if we can find the external column name
'            For Each destx_col As IDTSExternalMetadataColumn100 In dest_input.ExternalMetadataColumnCollection

'                'handle matching converted column names
'                Dim vcol_to_compare As String = vColumn.Name
'                vcol_to_compare = vcol_to_compare.Replace("_conv", "")

'                If UCase(destx_col.Name) = UCase(vcol_to_compare) Then
'                    curr_destx_col_id = destx_col.ID
'                    Exit For
'                End If

'            Next


'            'wire it up
'            dest_inst.MapInputColumn(dest_input.ID, vCol.ID, curr_destx_col_id)


'skip_wireup:
'        Next

'        If need_conversion = True Then
'            dest_inst.ReinitializeMetaData()
'        End If

'        dest_inst.ReleaseConnections()
'        dest_inst = Nothing

'        '---------------------------------------------------------------
'        'execute

'        Dim pkgResults As Microsoft.SqlServer.Dts.Runtime.DTSExecResult
'        pkgResults = pkg.Execute()

'        app.SaveToXml("C:\temp\test_xl.dtsx", pkg, Nothing)

'        If pkgResults = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure Then
'            Dim err_msg As String = ""
'            For Each Err As DtsError In pkg.Errors
'                err_msg += Err.Description.ToString
'            Next
'            ' MsgBox(err_msg)
'        End If



'    End Sub

'    Class src_meta

'        Private c_col_nm As String
'        Private c_col_data_type As String
'        Private c_col_length As Integer
'        Private c_col_ssis_data_type As DataType
'        Private c_col_prec As Integer
'        Private c_col_scale As Integer
'        Private c_col_ssis_data_type_dest As DataType
'        Private c_col_needs_conversion As Boolean
'        Private c_col_length_dest As Integer
'        Private c_col_prec_dest As Integer
'        Private c_col_scale_dest As Integer
'        Private c_col_code_page_dest As Integer

'        Property col_needs_conversion() As Boolean
'            Get
'                Return c_col_needs_conversion
'            End Get
'            Set(value As Boolean)
'                c_col_needs_conversion = value
'            End Set
'        End Property

'        Property col_nm() As String
'            Get
'                Return c_col_nm
'            End Get
'            Set(value As String)
'                c_col_nm = value
'            End Set
'        End Property

'        Property data_type_str() As String
'            Get
'                Return c_col_data_type
'            End Get
'            Set(value As String)
'                c_col_data_type = value
'            End Set
'        End Property

'        Property col_length() As Integer
'            Get
'                Return c_col_length
'            End Get
'            Set(value As Integer)
'                c_col_length = value
'            End Set
'        End Property

'        Property col_length_dest() As Integer
'            Get
'                Return c_col_length_dest
'            End Get
'            Set(value As Integer)
'                c_col_length_dest = value
'            End Set
'        End Property

'        Property col_prec() As Integer
'            Get
'                Return c_col_prec
'            End Get
'            Set(value As Integer)
'                c_col_prec = value
'            End Set
'        End Property

'        Property col_prec_dest() As Integer
'            Get
'                Return c_col_prec_dest
'            End Get
'            Set(value As Integer)
'                c_col_prec_dest = value
'            End Set
'        End Property

'        Property col_scale() As Integer
'            Get
'                Return c_col_scale
'            End Get
'            Set(value As Integer)
'                c_col_scale = value
'            End Set
'        End Property

'        Property col_scale_dest() As Integer
'            Get
'                Return c_col_scale_dest
'            End Get
'            Set(value As Integer)
'                c_col_scale_dest = value
'            End Set
'        End Property

'        Property col_code_page_dest() As Integer
'            Get
'                Return c_col_code_page_dest
'            End Get
'            Set(value As Integer)
'                c_col_code_page_dest = value
'            End Set
'        End Property

'        Property data_type_obj() As DataType
'            Get
'                Return c_col_ssis_data_type
'            End Get
'            Set(value As DataType)
'                c_col_ssis_data_type = value
'            End Set
'        End Property

'        Property data_type_obj_dest() As DataType
'            Get
'                Return c_col_ssis_data_type_dest
'            End Get
'            Set(value As DataType)
'                c_col_ssis_data_type_dest = value
'            End Set
'        End Property

'    End Class

'    'Class to hold destination meta data column information
'    Class dest_meta

'        Private c_col_nm As String
'        Private c_col_data_type As String
'        Private c_col_length As Integer
'        Private c_col_ssis_data_type As DataType
'        Private c_col_prec As Integer
'        Private c_col_scale As Integer
'        Private c_col_code_page As Integer

'        Property col_nm() As String
'            Get
'                Return c_col_nm
'            End Get
'            Set(value As String)
'                c_col_nm = value
'            End Set
'        End Property

'        Property data_type_str() As String
'            Get
'                Return c_col_data_type
'            End Get
'            Set(value As String)
'                c_col_data_type = value
'            End Set
'        End Property

'        Property col_length() As Integer
'            Get
'                Return c_col_length
'            End Get
'            Set(value As Integer)
'                c_col_length = value
'            End Set
'        End Property

'        Property col_prec() As Integer
'            Get
'                Return c_col_prec
'            End Get
'            Set(value As Integer)
'                c_col_prec = value
'            End Set
'        End Property

'        Property col_scale() As Integer
'            Get
'                Return c_col_scale
'            End Get
'            Set(value As Integer)
'                c_col_scale = value
'            End Set
'        End Property

'        Property col_code_page() As Integer
'            Get
'                Return c_col_code_page
'            End Get
'            Set(value As Integer)
'                c_col_code_page = value
'            End Set
'        End Property

'        Property data_type_obj() As DataType
'            Get
'                Return c_col_ssis_data_type
'            End Get
'            Set(value As DataType)
'                c_col_ssis_data_type = value
'            End Set
'        End Property

'    End Class

'End Class
