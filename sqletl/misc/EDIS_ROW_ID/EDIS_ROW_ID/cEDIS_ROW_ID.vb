Imports Microsoft.SqlServer.Dts.Pipeline
Imports Microsoft.SqlServer.Dts.Pipeline.Wrapper
Imports Microsoft.SqlServer.Dts.Runtime.Wrapper
Namespace MDDataTech

    <DtsPipelineComponent(DisplayName:="EDIS Row Count", ComponentType:=ComponentType.Transform, Description:="EDIS Row Count Transform; www.sqletl.com")>
    Public Class EDIS_ROW_CONT
        Inherits PipelineComponent

        Private row_id As Long
        Private edis_row_id_lineage_id As Long
        Private Const debug_path As String = "C:\temp\custom_comp_debug.txt"

        Public Overrides Sub ProvideComponentProperties()

            'This loads the columns, but the data flow out does not recognize any inputs
            Dim output As IDTSOutput100 = ComponentMetaData.OutputCollection.New
            output.Name = "Output"
            Dim column As IDTSOutputColumn100 = output.OutputColumnCollection.New
            column.Name = "EDIS_SRC_ROW_ID"
            column.SetDataTypeProperties(DataType.DT_I8, 0, 0, 0, 0)
            column.Description = "EDIS Source Row ID. www.sqletl.com"
            edis_row_id_lineage_id = column.LineageID
            Dim input As IDTSInput100 = ComponentMetaData.InputCollection.[New]
            input.Name = "Input"

            'link input and output so they are synchronized
            output.SynchronousInputID = input.ID

        End Sub

        Public Overrides Sub PreExecute()
            row_id = 0

            If System.IO.File.Exists(debug_path) Then
                System.IO.File.Delete(debug_path)
            End If

            'grab the lineage id
            edis_row_id_lineage_id = ComponentMetaData.OutputCollection(0).OutputColumnCollection.Item(0).LineageID

        End Sub

        Public Overrides Sub ProcessInput(ByVal inputID As Integer, ByVal buffer As PipelineBuffer)

            'Get the buffer index for the EDIS output column
            Dim input As IDTSInput100 = ComponentMetaData.InputCollection.GetObjectByID(inputID)
            Dim edis_src_row_id_buffer_index = BufferManager.FindColumnByLineageID(input.Buffer, edis_row_id_lineage_id)

            While buffer.NextRow
                row_id += 1
                buffer.SetInt64(edis_src_row_id_buffer_index, row_id)
            End While
        End Sub

        Private Sub task_debug(msg As String)
            Dim file As System.IO.StreamWriter
            file = My.Computer.FileSystem.OpenTextFileWriter(debug_path, True)
            Dim output_msg As String = Now.ToLongTimeString + " -> " + msg
            file.WriteLine(output_msg)
            file.Close()
        End Sub

    End Class
End Namespace