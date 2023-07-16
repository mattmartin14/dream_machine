Imports System.IO

Module mLocalFile

    Sub local_file_main(ByRef err_msg As String)

        Select Case LCase(EDIS.get_edis_pval("sub_task"))
            Case "rename_file"
                Call rename_file(
                    EDIS.get_edis_pval("curr_file_path"),
                    EDIS.get_edis_pval("new_file_path"),
                    err_msg
                )

            Case "delete_files"
                Call delete_files(
                    EDIS.get_edis_pval("folder_path"),
                   EDIS.get_edis_pval("file_crit"),
                    err_msg,
                    EDIS.get_edis_pval("file_list_usr")
                )
            Case "get_file_list"
                Call get_file_list(
                    EDIS.get_edis_pval("folder_path"),
                    EDIS.get_edis_pval("file_crit"),
                    CBool(EDIS.get_edis_pval("show_details")),
                    EDIS.get_edis_pval("mssql_inst"),
                    EDIS.get_edis_pval("tmp_tbl_nm"),
                    err_msg
                )
            Case "create_dir"
                create_dir(EDIS.get_edis_pval("folder_path"), err_msg)
            Case "delete_dir"
                delete_dir(EDIS.get_edis_pval("folder_path"), err_msg)
            Case "copy_file"
                copy_file(EDIS.get_edis_pval("file_path"),
                         EDIS.get_edis_pval("folder_path"),
                          err_msg)
            Case "update_file_attribute"
                update_file_attribute(EDIS.get_edis_pval("file_path"), EDIS.get_edis_pval("attribute_name"), err_msg)

        End Select

    End Sub

    Private Sub update_file_attribute(ByVal file_path As String, ByVal attribute_name As String, ByRef err_msg As String)

        Try
            If Not File.Exists(file_path) Then
                err_msg = "File [" + file_path + "] does not exist."
                Exit Sub
            End If

            Select Case LCase(attribute_name)
                Case "hidden"
                    File.SetAttributes(file_path, FileAttributes.Hidden)
                Case "readonly"
                    File.SetAttributes(file_path, FileAttributes.ReadOnly)
            End Select
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try


    End Sub
    Private Sub copy_file(ByVal file_path As String, ByVal target_folder As String, ByRef err_msg As String)
        Try
            If File.Exists(file_path) And Directory.Exists(target_folder) Then

                If Right(target_folder, 1) <> "\" Then target_folder += "\"
                Dim new_file_path As String = target_folder + Path.GetFileName(file_path)
                File.Copy(file_path, new_file_path, True)
            Else
                err_msg = "File does not exist"
            End If
        Catch ex As Exception
            err_msg = ex.Message
        End Try

    End Sub

    Private Sub create_dir(ByVal folder_path As String, ByRef err_msg As String)
        Try
            If Directory.Exists(folder_path) Then
                err_msg = "Directory path [" + folder_path + "] already exists"
            Else
                Directory.CreateDirectory(folder_path)
            End If
        Catch ex As Exception
            err_msg = ex.Message
        End Try
    End Sub

    Private Sub delete_dir(ByVal folder_path As String, ByRef err_msg As String)
        Try
            If Not Directory.Exists(folder_path) Then
                err_msg = "Directory path [" + folder_path + "] does not exist"
            Else
                Directory.Delete(folder_path, True)
            End If
        Catch ex As Exception
            err_msg = ex.Message
        End Try
    End Sub

    Private Sub rename_file(ByVal curr_file_path As String, ByVal new_file_path As String, ByRef err_msg As String)

        Try
            If File.Exists(curr_file_path) Then
                File.Move(curr_file_path, new_file_path)
            End If
        Catch ex As Exception
            err_msg = ex.Message
        End Try

    End Sub

    '------------ Delete File ----------------------------
    Private Sub delete_files(ByVal folder_path As String, ByVal file_crit As String, ByRef err_msg As String, Optional ByRef file_list_usr As String = "")

        Try

            If Right(folder_path, 1) <> "\" Then folder_path = folder_path + "\"

            Dim dir_info As New DirectoryInfo(folder_path)

            Dim file_list As List(Of String)
            file_list = New List(Of String)

            If file_list_usr.Length > 0 Then
                Dim file_list_1() As String
                file_list_1 = file_list_usr.Split(";")
                For i As Integer = LBound(file_list_1) To UBound(file_list_1)
                    file_list.Add(folder_path & file_list_1(i))
                Next i
            Else
                Dim files As FileInfo() = dir_info.GetFiles(file_crit)

                For Each File As FileInfo In files
                    file_list.Add(File.FullName)
                Next

            End If

            If file_list.Count = 0 Then Exit Sub

            For Each file_path As String In file_list

                If File.Exists(file_path) Then
                    File.Delete(file_path)
                End If

            Next

        Catch ex As Exception
            err_msg = ex.Message
        End Try

    End Sub

    '------------ list files File ----------------------------

    Private Sub get_file_list(ByVal folder_path As String, _
                              ByVal file_crit As String, _
                              ByVal show_details As Boolean, _
                              ByVal mssql_inst As String, _
                              ByVal tmp_tbl_nm As String, _
                              ByRef err_msg As String)

        Try
            Dim dir_info As New DirectoryInfo(folder_path)
            Dim files As FileInfo() = dir_info.GetFiles(file_crit)

            If files Is Nothing Then Exit Sub

            Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = true")

                Dim sql_str As String
                If show_details Then
                    sql_str = _
                        "insert [" + tmp_tbl_nm + "] " + _
                        "values (@file_nm, @full_nm, @ext, @crt_ts, @last_axs_ts, @is_read_only, @file_size)"
                Else
                    sql_str = "insert [" + tmp_tbl_nm + "] values (@file_nm)"
                End If

                cn.Open()

                For Each File As FileInfo In files
                    Using cmd As New SqlClient.SqlCommand(sql_str, cn)
                        With cmd.Parameters
                            .AddWithValue("@file_nm", File.Name)
                            If show_details Then
                                .AddWithValue("@full_nm", File.FullName)
                                .AddWithValue("@ext", File.Extension)
                                .AddWithValue("@crt_ts", File.CreationTime)
                                .AddWithValue("@last_axs_ts", File.LastAccessTime)
                                .AddWithValue("@is_read_only", File.IsReadOnly)
                                .AddWithValue("@file_size", File.Length / 1024)
                            End If
                        End With
                        cmd.ExecuteNonQuery()
                    End Using
                Next
            End Using

        Catch ex As Exception

            err_msg = ex.Message

        End Try

    End Sub

End Module