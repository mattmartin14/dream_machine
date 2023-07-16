
Imports System.IO
Imports Rebex.IO.Compression
Class cZip

    Shared Sub zip_main(ByRef err_msg As String)

        Dim sub_task As String = EDIS.get_edis_pval("sub_task")
        Dim folder_path As String = EDIS.get_edis_pval("folder_path")
        Dim file_path As String = EDIS.get_edis_pval("file_path")
        Dim zip_file_path As String = EDIS.get_edis_pval("zip_file_path")


        Select Case sub_task
            Case "unzip_files"
                unzip_files(zip_file_path, folder_path, err_msg)
            Case "zip_files"
                Dim append_to_file As Boolean = CBool(EDIS.get_edis_pval("append_to_existing_file"))
                Dim include_sub_folders As Boolean = CBool(EDIS.get_edis_pval("include_sub_folders"))
                zip_files(folder_path, zip_file_path, include_sub_folders, append_to_file, err_msg)
        End Select

    End Sub

    Friend Shared Sub zip_files(folder_path As String, zip_file_path As String, include_sub_folders As Boolean, ByVal append_to_file As Boolean, ByRef err_msg As String)
        Try
            If File.Exists(zip_file_path) And append_to_file = False Then
                File.Delete(zip_file_path)
            End If

            'set the traversal mode (default to just parent directory)
            Dim tMode As Rebex.IO.TraversalMode = Rebex.IO.TraversalMode.MatchFilesShallow

            If include_sub_folders Then
                tMode = Rebex.IO.TraversalMode.Recursive
            End If

            Using zip As New ZipArchive(zip_file_path)
                zip.Add(folder_path, Nothing, tMode)
            End Using

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try
    End Sub

    Private Shared Sub unzip_files(zip_file_path As String, unzip_file_path As String, ByRef err_msg As String)

        Try

            If Not File.Exists(zip_file_path) Then
                err_msg = "Error Unzip: File [" + zip_file_path + "] does not exist"
                Exit Sub
            End If

            If Not Directory.Exists(unzip_file_path) Then
                Directory.CreateDirectory(unzip_file_path)
            End If

            Using zip As New ZipArchive(zip_file_path)
                zip.ExtractAll(unzip_file_path, Rebex.IO.TransferMethod.Copy, Rebex.IO.ActionOnExistingFiles.OverwriteAll)
            End Using

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

    End Sub


End Class
