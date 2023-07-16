Module mEntry

    Enum retCode
        Success = 0
        TaskError = 1
        LauncherError = 2
        UnknownError = 3
    End Enum


    Function Main(args() As String) As Integer

        Dim err_msg As String


        Try

            '############# TESTING ################
            ReDim args(3)
            args(0) = "MDDT\DEV16"
            args(1) = "123mm"
            args(2) = "server = MDDT\DEV16; Integrated Security = TRUE"
            args(3) = ""
            '############# TESTING ################


            Dim mssql_inst As String = args(0).ToString
            Dim exec_id As String = args(1).ToString
            Dim sString1 As String = args(2).ToString
            Dim sString2 As String = args(3).ToString


            cParams.load_params(mssql_inst, exec_id, sString1, sString2)

            'load sql instance if it wasnt passed in
            If Not cParams._edis_params.ContainsKey("mssql_inst") Then
                cParams._edis_params.Add("mssql_inst", mssql_inst)
            End If


            Dim primary_task As String = cParams.get_param_val("primary_task")
            Select Case primary_task
                Case "excel"
                    cExcelMain.main(err_msg)
            End Select

            If err_msg <> "" Then
                Console.WriteLine(err_msg)
                Return retCode.TaskError
            End If

            Return retCode.Success

        Catch ex As Exception
            Console.WriteLine(ex.Message.ToString)
            Return retCode.LauncherError
        End Try



    End Function

End Module
