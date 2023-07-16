Imports System.Text
Imports System.Diagnostics
Class cProcessTask


    '*********** Notes about process task ***********'

    '[1] If you run something like bcp and it goes into an unresponsive state, it won't output an error, it will just hang at the waitforexit command
    '       -> as long as you redirect the input stream, it will make sure the process terminates
    '       -> NEED TO ADD A CHECKER AFTER THE PROCESS STARTS TO SEE IF A SYSTEM ERROR OCCURED, THEN IF SO, TERM THE PROCESS
    '[2] Sometimes error outputs go into the standard output, so you need to check both

    Private Shared outData As StringBuilder = New StringBuilder
    Private Shared errData As StringBuilder = New StringBuilder

    Shared Sub exec_process_task(ByRef err_msg As String)


        Dim process_nm As String = EDIS.get_edis_pval("process_nm")
        Dim args As String = EDIS.get_edis_pval("args")
        Dim output_tbl As String = EDIS.get_edis_pval("output_tbl")
        Dim mssql_inst As String = EDIS.get_edis_pval("mssql_inst")

        '//////////// TESTING //////////////////////
        'bcp that fails but reports error
        'process_nm = "bcp.exe"
        'args = """select firstName, LastName from adventureworks.person.person"" queryout C:\temp\blah2.csv -T MDDT\DEV14"

        ''bcp that fails and hangs on waitforexit (no error reporting)
        'process_nm = "bcp.exe"
        'args = """select firstName, LastName from adventureworks.person.person"" queryout C:\temp\blah2.csv -T -S MDDT\DEV14"

        'succeeds
        ' process_nm = "powershell.exe"
        'args = "net localgroup administrators"

        'bcp that suceeeds'
        ' process_nm = "bcp.exe"
        ' args = "adventureworks.person.person out C:\temp\testmm123.csv -T -S MDDT\DEV14 -t -c"

        'force an error on a normal program
        'args = "net localgroup blah"

        'Note: If trying to run cmd.exe, for the args, add the /c operator e.g. "cmd.exe", @args = "/c net localgroup administrators"; otherwise, it hangs

        '///////////////////////////// END TESTING /////////////////////////


        'Begin Code

        Dim p As New Process

        Try

            'Note: Adding to redirect standard input = true will allow the process to terminate if it hangs
            With p.StartInfo
                .FileName = process_nm
                .Arguments = args
                .RedirectStandardError = True
                .RedirectStandardOutput = True
                .RedirectStandardInput = True
                .UseShellExecute = False
                .WindowStyle = ProcessWindowStyle.Hidden
                .CreateNoWindow = True
            End With

            p.EnableRaisingEvents = True

            'add asynchronous handlers for output and error streams
            AddHandler p.OutputDataReceived, AddressOf process_output_statements
            AddHandler p.ErrorDataReceived, AddressOf process_error_statements

            p.Start()

            'start async read before the waitfor exit, or you get deadlock
            p.BeginOutputReadLine()
            p.BeginErrorReadLine()

            p.WaitForExit()

            'for success
            If p.ExitCode = 0 And Trim(output_tbl) <> "" Then

                Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = true")
                    cn.Open()

                    Dim cmd_str As String = "INSERT " + output_tbl + " VALUES(@res)"

                    Dim res() As String = outData.ToString.Split(vbCrLf)

                    For Each entry As String In res
                        Using cmd As New SqlClient.SqlCommand(cmd_str, cn)
                            cmd.Parameters.AddWithValue("@res", entry)
                            cmd.ExecuteNonQuery()
                        End Using
                    Next

                    cn.Close()
                End Using

            ElseIf p.ExitCode <> 0 Then

                err_msg = errData.ToString()

                'sometimes errors are redirected to the main output stream
                If Trim(err_msg) = "" Then
                    err_msg = outData.ToString()
                End If

                If Trim(err_msg) = "" Then
                    err_msg = "An unknown error occured while trying to process the command. Please check your process task args before resubmitting."
                End If
                Exit Sub
            End If

        Catch ex As Exception
            err_msg = "Error: Process task did not return success. Error message > " + ex.Message.ToString()
        Finally

            'free the process thread

            'don't use kill because it can error sometimes if the process already aborted.
            ' p.Kill()
            p.Close()
            p.Dispose()

        End Try

    End Sub

    'handlers for asynchronous outputs
    Private Shared Sub process_output_statements(sender As Object, out_line As DataReceivedEventArgs)

        If Not String.IsNullOrEmpty(out_line.Data) Then
            outData.AppendLine(out_line.Data.ToString())
        End If

    End Sub

    Private Shared Sub process_error_statements(sender As Object, out_line As DataReceivedEventArgs)

        If Not String.IsNullOrEmpty(out_line.Data) Then
            errData.AppendLine(out_line.Data.ToString())
        End If

    End Sub

End Class
