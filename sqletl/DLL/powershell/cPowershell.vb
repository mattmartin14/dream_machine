Imports System.Collections.ObjectModel
Imports System.Management.Automation
Imports System.Management.Automation.Runspaces
Friend Class cPowershell


    Friend Shared Sub run_powershell_script(ByRef err_msg As String)

        Dim err_prefix As String = "Powershell Script Error: "

        Try

            Dim ps_string As String = EDIS.get_edis_pval("script").ToString()
            Dim output_tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm").ToString()

            Dim runspace As Runspace = RunspaceFactory.CreateRunspace()

            runspace.Open()

            Dim pipeline As Pipeline = runspace.CreatePipeline()

            pipeline.Commands.AddScript(ps_string)

            pipeline.Commands.Add("Out-String")

            Dim res_collection As Collection(Of PSObject) = pipeline.Invoke()

            runspace.Close()

            '------------------------------------------------------------------------------------------
            'Error Check

            If pipeline.Error.Count > 0 Then

                err_msg = err_prefix

                While Not pipeline.Error.EndOfPipeline

                    Dim err_obj As PSObject = pipeline.Error.Read()

                    If err_obj IsNot Nothing Then
                        Dim err_base_object As ErrorRecord = err_obj.BaseObject

                        If err_base_object IsNot Nothing Then
                            err_msg += err_base_object.Exception.Message + vbCrLf
                        End If


                    End If

                End While

                'Console.WriteLine("Error: {0}", err_msg)

                Exit Sub
            End If

            'Dim results_str As New StringBuilder()


            'send back results if applicable
            If Trim(output_tbl_nm) <> "" Then

                Using cn As New SqlClient.SqlConnection("server = " + EDIS._mssql_inst + "; integrated security = true")
                    cn.Open()

                    Dim cmd_str As String = "INSERT " + output_tbl_nm + " VALUES(@res)"

                    For Each itm As PSObject In res_collection

                        Dim lines() = itm.ToString.Split(vbCrLf)

                        For Each line In lines
                            If Not String.IsNullOrWhiteSpace(line) Then
                                line = Replace(Trim(line), vbCrLf, "")
                                Using cmd As New SqlClient.SqlCommand(cmd_str, cn)
                                    cmd.Parameters.AddWithValue("@res", line.ToString)
                                    cmd.ExecuteNonQuery()
                                End Using
                            End If
                        Next


                    Next

                        cn.Close()
                End Using

            End If
        Catch ex As Exception

            err_msg = err_prefix + ex.Message.ToString()


        End Try
    End Sub

End Class
