Friend Class cExcelMain

    Shared Sub main(ByRef err_msg As String)

        'Is this excel03 or 07?
        Dim file_path As String = cParams.get_param_val("file_path")

        If Right(file_path, 4) = ".xls" Then
            cExcel03.main(err_msg)
        Else
            cExcel07.main(err_msg)
        End If

        If err_msg <> "" Then
            err_msg = "Excel Error: Task > " + cParams.get_param_val("sub_task") + " > Error Message: " + err_msg
        End If

    End Sub

End Class
