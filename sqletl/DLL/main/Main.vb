Public Module Main
    Enum EDISOutcome
        success = 0
        task_error = 1
        startup_error = 2
        unknown_error = 3
    End Enum

    Friend show_call_stack As Boolean = False


    Friend _mssql_inst As String
    Friend _exec_id As String
    Friend _turn_on_logging As Boolean = False
    Private ret_cd As Integer
    Private launcher_err_msg As String

    Public Function Main(args() As String) As Integer

        Try
            'INPUTS
            '0 > MS SQL Instance
            '1 > Exec ID
            '2 > Logging Level


            '#####################################################################
            '### TESTING


            'ReDim args(2)

            'args(0) = "MDDT\DEV16"
            'args(1) = "tmp123mm"
            'args(2) = "VERBOSE"

            '_mssql_inst = args(0)

            'Dim err_msg As String

            'show_call_stack = True

            ''Dim src_cn_str As String = "Provider=SQLOLEDB; Data Source = MDDT\DEV16; Integrated Security = SSPI"
            ''cData_Transfer_Shell.run_data_transfer_db_to_ff_out(src_cn_str, "OLEDB", "select top 3 * From adventureworks.person.person", "C:\temp\matt1.csv",
            ''                                                    "|", "{CR}{LF}", """", False, err_msg)


            'cXL07.import_data("C:\temp\testxl07.xlsx", "blah1", True, "sandbox", "dbo", "err_sheet", "MDDT\DEV16", False, True, 1000, False, 1000, 0, False, err_msg)

            'If err_msg <> "" Then
            '    Throw New Exception(err_msg)
            'End If

            ''Dim query As String = "Select top 1000 * from adventureworks.sales.SalesOrderHeader"
            ''cDT_Tasks.test_data_type_guess(query, 1000)

            'Console.WriteLine("press enter to exit...")

            'Do While Console.ReadKey().Key <> ConsoleKey.Enter

            'Loop

            'Exit Function

            '####################################################################

            _mssql_inst = args(0)
            _exec_id = args(1)

            If args(2).ToLower = "verbose" Then
                _turn_on_logging = True
            End If

            log_edis_event("info", "Startup", "Exec ID [" + _exec_id + "] assigned")
            log_edis_event("info", "Startup", "MSSQL Instance [" + _mssql_inst + "] assigned")

            EDIS.eMain()

            ''----------------------------------------------------------------------
            ''TEST
            'ret_cd = 2
            'launcher_err_msg = "test error that this happened"
            'Console.WriteLine("Error: {0}", launcher_err_msg)
            'Return EDISOutcome.startup_error
            'Exit Function

            ''END TEST
            '------------------------------------------------------------------

            If EDIS._edis_err_msg = "" Then
                ret_cd = 0
                Return EDISOutcome.success
            Else
                Console.WriteLine("Error: {0}", EDIS._edis_err_msg.ToString)
                ret_cd = 1
                Return EDISOutcome.task_error
            End If




        Catch ex As Exception
            ret_cd = 2
            launcher_err_msg = ex.Message.ToString()
            Console.WriteLine("Error: {0}", launcher_err_msg)
            Return EDISOutcome.startup_error
            'Finally
            '    If ret_cd = 1 Then
            '        Throw New Exception(EDIS._edis_err_msg.ToString)
            '    ElseIf ret_cd = 2 Then
            '        Throw New Exception(launcher_err_msg)
            '    End If
        End Try

    End Function

    Sub log_edis_event(ByVal log_type As String, ByVal msg_hdr As String, ByVal msg_dtl As String)

        If _turn_on_logging = False Then Exit Sub

        Using cn As New SqlClient.SqlConnection("Server = " + _mssql_inst + "; Integrated Security = True")
            cn.Open()

            Dim sql_cmd As String = "EXEC SSISDB.EDIS.isp_log_task @exec_id, @log_type, @msg_hdr, @msg_dtl"

            Using cmd As New SqlClient.SqlCommand(sql_cmd, cn)
                cmd.Parameters.AddWithValue("@exec_id", _exec_id)
                cmd.Parameters.AddWithValue("@log_type", log_type)
                cmd.Parameters.AddWithValue("@msg_hdr", msg_hdr)
                cmd.Parameters.AddWithValue("@msg_dtl", msg_dtl)
                cmd.ExecuteNonQuery()
            End Using
            cn.Close()
        End Using
    End Sub

End Module
