Imports System.IO
Imports Rebex.IO.Compression
Class cRunInstallScripts

    Shared Sub active_license(ByVal mssql_inst As String, ByVal serial_nbr As String, ByRef err_msg As String, ByVal action As String)

        Try

            'write out exe to temp path
            Dim temp_work_path As String = System.IO.Path.GetTempPath

            Dim temp_exe_zip As String = temp_work_path + "edis_installer.zip"

            Dim temp_exe_unzip_path As String = temp_work_path + "EDIS_EXE_UNZIPPED\"

            Dim err_log As String = System.IO.Path.GetTempFileName

            File.WriteAllBytes(temp_exe_zip, My.Resources.Resource.EDIS_EXE)

            'unzip it
            If Not Directory.Exists(temp_exe_unzip_path) Then
                Directory.CreateDirectory(temp_exe_unzip_path)
            End If

            Using zip As New ZipArchive(temp_exe_zip)
                zip.ExtractAll(temp_exe_unzip_path, Rebex.IO.TransferMethod.Copy, Rebex.IO.ActionOnExistingFiles.OverwriteAll)
            End Using

            Dim args As String = String.Format("""{0}"" ""{1}"" ""{2}"" ""{3}""", mssql_inst, serial_nbr, err_log, action)

            'Dim res As Integer
            Dim cmd As String = temp_exe_unzip_path + "EDIS_EXE\EDIS_EXE.exe " + args
            'Dim cmd As String = temp_exe + " " + args

            Shell(cmd, AppWinStyle.Hide, True)

            'check error
            Dim data As String
            Using sr As New StreamReader(err_log)
                data = sr.ReadToEnd()
            End Using


            data = Trim(data)

            If Left(LCase(data), 6) = "error:" Then
                err_msg = "SQL Objects Install Error: " + data
                Exit Sub
            ElseIf Left(LCase(data), 8) = "offline:" Then
                err_msg = data
                Exit Sub
            End If



            If File.Exists(temp_exe_zip) Then
                File.Delete(temp_exe_zip)
            End If
            'If Directory.Exists(temp_exe_unzip_path) Then
            '    Directory.Delete(temp_exe_unzip_path, True)
            'End If
            If File.Exists(err_log) Then
                File.Delete(err_log)
            End If

        Catch ex As Exception
            err_msg = ex.Message
            'MsgBox(ex.Message)
        End Try

    End Sub

End Class
