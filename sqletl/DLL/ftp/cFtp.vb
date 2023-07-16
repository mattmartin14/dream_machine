
Imports System.Data.SqlClient
Imports System.IO
Class cftp

    Shared Sub ftp_main(ByRef err_msg As String)

        Dim task As String = LCase(EDIS.get_edis_pval("sub_task"))

        Dim ftp_uid As String = EDIS.get_edis_pval("ftp_uid")
        Dim ftp_pwd As String = EDIS.get_edis_pval("ftp_pwd")

        'Dim ftp_uid As String = EDIS.get_sensitive_value("svc_uid", EDIS.get_edis_pval("ftp_id"))
        'If EDIS._edis_err_msg <> "" Then Exit Sub
        'Dim ftp_pwd As String = EDIS.get_sensitive_value("svc_pwd", EDIS.get_edis_pval("ftp_id"))
        'If EDIS._edis_err_msg <> "" Then Exit Sub

        Select Case task
            Case "download_files"
                ftp_download(EDIS.get_edis_pval("ftp_srvr"),
                        EDIS.get_edis_pval("ftp_dir"),
                        EDIS.get_edis_pval("file_crit"),
                        EDIS.get_edis_pval("file_list_usr"),
                        EDIS.get_edis_pval("local_dir"),
                        ftp_uid,
                        ftp_pwd,
                        EDIS.get_edis_pval("ftp_port_nbr"),
                        CBool(EDIS.get_edis_pval("use_ssl")),
                        CBool(EDIS.get_edis_pval("use_ssh")),
                        CBool(EDIS.get_edis_pval("use_passive")),
                        err_msg
                )

            Case "upload_files"
                ftp_upload(EDIS.get_edis_pval("ftp_srvr"),
                            EDIS.get_edis_pval("ftp_dir"),
                            EDIS.get_edis_pval("file_crit"),
                            EDIS.get_edis_pval("file_list_usr"),
                            EDIS.get_edis_pval("local_dir"),
                            ftp_uid,
                            ftp_pwd,
                            EDIS.get_edis_pval("archive_folder"),
                            EDIS.get_edis_pval("archive_flag"),
                            CBool(EDIS.get_edis_pval("use_binary")),
                           EDIS.get_edis_pval("ftp_port_nbr"),
                            CBool(EDIS.get_edis_pval("use_ssl")),
                            CBool(EDIS.get_edis_pval("use_ssh")),
                            CBool(EDIS.get_edis_pval("use_passive")),
                            err_msg
                            )


            Case "delete_files"
                ftp_delete(EDIS.get_edis_pval("ftp_srvr"),
                            EDIS.get_edis_pval("ftp_dir"),
                            EDIS.get_edis_pval("file_crit"),
                            EDIS.get_edis_pval("file_list_usr"),
                            ftp_uid,
                            ftp_pwd,
                            EDIS.get_edis_pval("ftp_port_nbr"),
                            CBool(EDIS.get_edis_pval("use_ssl")),
                            CBool(EDIS.get_edis_pval("use_ssh")),
                            CBool(EDIS.get_edis_pval("use_passive")),
                            err_msg
                )

            Case "get_file_list"
                ftp_list_content(EDIS.get_edis_pval("ftp_srvr"),
                            EDIS.get_edis_pval("ftp_dir"),
                            EDIS.get_edis_pval("file_crit"),
                            ftp_uid,
                            ftp_pwd,
                            EDIS.get_edis_pval("mssql_inst"),
                            EDIS.get_edis_pval("tmp_tbl_nm"),
                            CBool(EDIS.get_edis_pval("show_details")),
                            EDIS.get_edis_pval("ftp_port_nbr"),
                            CBool(EDIS.get_edis_pval("use_ssl")),
                            CBool(EDIS.get_edis_pval("use_ssh")),
                            CBool(EDIS.get_edis_pval("use_passive")),
                            err_msg
                )


            Case "create_dir"
                ftp_create_dir(EDIS.get_edis_pval("ftp_srvr"),
                            EDIS.get_edis_pval("ftp_dir"),
                            EDIS.get_edis_pval("folder_nm"),
                            ftp_uid,
                            ftp_pwd,
                            EDIS.get_edis_pval("ftp_port_nbr"),
                            CBool(EDIS.get_edis_pval("use_ssl")),
                            CBool(EDIS.get_edis_pval("use_ssh")),
                            CBool(EDIS.get_edis_pval("use_passive")),
                            err_msg
                )


        End Select

    End Sub

    Shared Sub ftp_upload(ByVal ftp_srvr As String, ByVal ftp_dir As String, ByVal file_crit As String, ByVal file_list_usr As String,
                             ByVal local_dir As String, ByVal uid As String,
                             ByVal pwd As String, ByVal archive_folder As String, archive_flag As Boolean, ByVal use_binary As Boolean,
                             ByVal port_nbr As Integer, ByVal use_ssl As Boolean, ByVal use_ssh As Boolean, ByVal use_passive As Boolean, ByRef err_msg As String)

        'QA Check List
        '   [1] Upload with file criteria - PASS
        '   [2] Upload with file name list - PASS
        '   [3] upload with file criteria and archive - PASS
        '   [4] upload with file name list and archive - PASS

        Try

            If Right(local_dir, 1) <> "\" Then local_dir += "\"
            If Right(ftp_dir, 1) <> "/" Then ftp_dir += "/"

            Dim ftp
            If use_ssh Then
                ftp = New Rebex.Net.Sftp
            Else
                ftp = New Rebex.Net.Ftp
            End If

            If port_nbr <> 0 Then
                ftp.Connect(ftp_srvr, port_nbr)
            Else
                ftp.Connect(ftp_srvr)
            End If

            If use_ssl And Not use_ssh Then ftp.Secure()

            ftp.Login(uid, pwd)

            'passive mode does not apply to sftp
            If use_passive And Not use_ssh Then ftp.Passive = True

            If use_binary = True Then
                ftp.TransferType = Rebex.Net.FtpTransferType.Binary
            End If

            Dim fset_loc_orig = New Rebex.IO.FileSet(local_dir)
            Dim fset_loc_archive_rename = New Rebex.IO.FileSet(local_dir)

            Dim ftp_dir_orig As String = ftp_dir
            Dim fset_ftp_orig = New Rebex.IO.FileSet(ftp_dir_orig)

            'Are we archiving the files? If so, rename them

            If archive_flag = True Then

                'were we given a file list or is it a wildcard?
                If file_list_usr.Length > 0 Then

                    'convert delimited list to fileset
                    'the list should be just the file name, not a full path e.g. "a1.txt"
                    Dim file_list As List(Of String) = file_list_usr.Split(";").ToList
                    For Each file_nm As String In file_list
                        fset_loc_orig.Include(file_nm, Rebex.IO.TraversalMode.MatchFilesShallow)
                    Next

                Else
                    'no list, just wildcard
                    fset_loc_orig.Include(Trim(file_crit), Rebex.IO.TraversalMode.MatchFilesShallow)
                End If

                'rename the local files
                For Each file_nm In fset_loc_orig.GetLocalItems

                    'load to the parent ftp directory file set group so we can delete later
                    fset_ftp_orig.Include(Trim(file_nm.Name), Rebex.IO.TraversalMode.MatchFilesShallow)

                    Dim f_path As String = file_nm.FullPath
                    Dim new_file_nm As String =
                        Path.GetFileNameWithoutExtension(f_path) & "_prcd_" & DateTime.Now.ToString("yyyyMMddHHmmss") & Path.GetExtension(f_path)

                    My.Computer.FileSystem.RenameFile(f_path, new_file_nm)
                    fset_loc_archive_rename.Include(new_file_nm, Rebex.IO.TraversalMode.MatchFilesShallow)
                Next

                'create the archive directory if it does not exist
                ftp.ChangeDirectory(ftp_dir)

                If Not ftp.DirectoryExists(archive_folder) Then
                    ftp.CreateDirectory(archive_folder)
                End If

                'update the ftp directory
                ftp_dir += archive_folder + "/"
                ftp.ChangeDirectory(ftp_dir)

                'Upload the files
                'note: some servers don't support multiple commands at once, thus we loop
                For Each loc_file In fset_loc_archive_rename.GetLocalItems
                    If loc_file.IsFile Then
                        ftp.PutFile(loc_file.FullPath, ftp_dir + loc_file.Name)
                    End If
                Next

                'jump back to parent directory and purge the existing non-renamed files
                ftp.ChangeDirectory(ftp_dir_orig)
                ftp.Delete(fset_ftp_orig)

                '----------------------------------------------------------------------
                'no archive
            Else

                If file_list_usr.Length > 0 Then
                    Dim file_list As List(Of String) = file_list_usr.Split(";").ToList
                    For Each file_nm As String In file_list
                        fset_loc_orig.Include(file_nm, Rebex.IO.TraversalMode.MatchFilesShallow)
                    Next
                Else
                    fset_loc_orig.Include(Trim(file_crit), Rebex.IO.TraversalMode.MatchFilesShallow)
                End If

                ftp.ChangeDirectory(ftp_dir)

                'note: some servers don't support multiple commands at once, thus we loop
                For Each loc_file In fset_loc_orig.GetLocalItems
                    If loc_file.IsFile Then
                        ftp.PutFile(loc_file.FullPath, ftp_dir + loc_file.Name)
                    End If
                Next

            End If

            ftp.Disconnect()

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try
    End Sub

    Shared Sub ftp_delete(ByVal ftp_srvr As String, ByVal ftp_dir As String, ByVal file_crit As String, ByVal file_list_usr As String,
                           ByVal uid As String, ByVal pwd As String, ByVal port_nbr As Integer,
                          ByVal use_ssl As Boolean, ByVal use_ssh As Boolean, ByVal use_passive As Boolean, ByRef err_msg As String)
        Try
            If Right(ftp_dir, 1) <> "/" Then
                ftp_dir = ftp_dir + "/"
            End If

            Dim ftp
            If use_ssh Then
                ftp = New Rebex.Net.Sftp
            Else
                ftp = New Rebex.Net.Ftp
            End If

            If port_nbr <> 0 Then
                ftp.Connect(ftp_srvr, port_nbr)
            Else
                ftp.Connect(ftp_srvr)
            End If

            If use_ssl And Not use_ssh Then ftp.Secure()

            ftp.Login(uid, pwd)

            'passive mode does not apply to sftp
            If use_passive And Not use_ssh Then ftp.Passive = True

            Dim fset = New Rebex.IO.FileSet(ftp_dir)

            If file_list_usr.Length > 0 Then
                Dim file_list1 As List(Of String) = file_list_usr.Split(";").ToList

                For Each file_nm As String In file_list1
                    fset.Include(Trim(file_nm), Rebex.IO.TraversalMode.MatchFilesShallow)
                Next
            Else
                fset.Include(file_crit, Rebex.IO.TraversalMode.MatchFilesShallow)
            End If

            ftp.ChangeDirectory(ftp_dir)

            ftp.Delete(fset)

            ftp.Disconnect()

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try
    End Sub

    Shared Sub ftp_download(ByVal ftp_srvr As String, ByVal ftp_dir As String, ByVal file_crit As String, ByVal file_list_usr As String,
                             ByVal local_dir As String, ByVal uid As String,
                             ByVal pwd As String, ByVal port_nbr As Integer,
                             ByVal use_ssl As Boolean, ByVal use_ssh As Boolean, ByVal use_passive As Boolean, ByRef err_msg As String)

        Try
            If Right(local_dir, 1) <> "\" Then
                local_dir = local_dir + "\"
            End If

            If Right(ftp_dir, 1) <> "/" Then
                ftp_dir = ftp_dir + "/"
            End If

            Dim ftp
            If use_ssh Then
                ftp = New Rebex.Net.Sftp
            Else
                ftp = New Rebex.Net.Ftp
            End If

            If port_nbr <> 0 Then
                ftp.Connect(ftp_srvr, port_nbr)
            Else
                ftp.Connect(ftp_srvr)
            End If

            If use_ssl And Not use_ssh Then ftp.Secure()

            ftp.Login(uid, pwd)

            'passive mode does not apply to sftp
            If use_passive And Not use_ssh Then ftp.Passive = True

            Dim fset = New Rebex.IO.FileSet(ftp_dir)
            If file_list_usr.Length > 0 Then
                Dim file_list As List(Of String) = file_list_usr.Split(";").ToList
                For Each file_nm As String In file_list
                    fset.Include(Trim(file_nm), Rebex.IO.TraversalMode.MatchFilesShallow)
                Next
            Else
                fset.Include(Trim(file_crit), Rebex.IO.TraversalMode.MatchFilesShallow)
            End If

            ftp.ChangeDirectory(ftp_dir)

            ftp.Download(fset, local_dir, Rebex.IO.TransferMethod.Copy, Rebex.IO.ActionOnExistingFiles.OverwriteAll)

            ftp.Disconnect()

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

    End Sub


    Shared Sub ftp_list_content(ByVal ftp_srvr As String, ByVal ftp_dir As String, ByVal file_crit As String, uid As String, pwd As String,
                                 ByVal mssql_inst As String, ByVal tmp_tbl As String, ByVal show_details As Boolean, ByVal port_nbr As Integer,
                                  ByVal use_ssl As Boolean, ByVal use_ssh As Boolean, ByVal use_passive As Boolean, ByRef err_msg As String)
        Try
            If Right(ftp_dir, 1) <> "/" Then
                ftp_dir = ftp_dir + "/"
            End If

            Dim ftp
            If use_ssh Then
                ftp = New Rebex.Net.Sftp
            Else
                ftp = New Rebex.Net.Ftp
            End If

            If port_nbr <> 0 Then
                ftp.Connect(ftp_srvr, port_nbr)
            Else
                ftp.Connect(ftp_srvr)
            End If

            If use_ssl And Not use_ssh Then ftp.Secure()

            ftp.Login(uid, pwd)

            'passive mode does not apply to sftp
            If use_passive And Not use_ssh Then ftp.Passive = True

            Dim folder_items
            If use_ssh Then
                folder_items = New Rebex.Net.SftpItemCollection
            Else
                folder_items = New Rebex.Net.FtpItemCollection
            End If
            
            ftp.ChangeDirectory(ftp_dir)
            
            'folder_items = ftp.GetList(ftp_dir)
            folder_items = ftp.GetItems(ftp_dir + file_crit, Rebex.IO.TraversalMode.MatchFilesShallow)
            
            ftp.Disconnect()

            Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = true")

                cn.Open()
                Dim sql_str As String
                If show_details Then
                    sql_str = "insert [" + tmp_tbl + "] values (@file_nm, @file_size, @file_last_mod_ts)"
                Else
                    sql_str = "insert [" + tmp_tbl + "] values (@file_nm)"
                End If

                For Each itm In folder_items

                    If Not itm.IsDirectory() Then
                        'fix this to compare the file criteria using lcase and like
                        Using cmd As New SqlClient.SqlCommand(sql_str, cn)
                            With cmd.Parameters
                                .AddWithValue("@file_nm", itm.Name)
                                If show_details = True Then
                                    .AddWithValue("@file_last_mod_ts", itm.LastWriteTime)
                                    .AddWithValue("@file_size", itm.Length / 1024)
                                End If
                            End With
                            cmd.ExecuteNonQuery()
                        End Using
                    End If
                Next
            End Using

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try
    End Sub

    Shared Sub ftp_create_dir(ByVal ftp_srvr As String, ByVal ftp_dir As String, ByVal folder_nm As String,
                               ByVal uid As String, ByVal pwd As String, ByVal port_nbr As Integer,
                               ByVal use_ssl As Boolean, ByVal use_ssh As Boolean, ByVal use_passive As Boolean, ByRef err_msg As String)
        Try
            If Right(ftp_dir, 1) <> "/" Then
                ftp_dir = ftp_dir + "/"
            End If

            Dim ftp
            If use_ssh Then
                ftp = New Rebex.Net.Sftp
            Else
                ftp = New Rebex.Net.Ftp
            End If

            If port_nbr <> 0 Then
                ftp.Connect(ftp_srvr, port_nbr)
            Else
                ftp.Connect(ftp_srvr)
            End If

            If use_ssl And Not use_ssh Then ftp.Secure()

            ftp.Login(uid, pwd)

            'passive mode does not apply to sftp
            If use_passive And Not use_ssh Then ftp.Passive = True

            ftp.ChangeDirectory(ftp_dir)

            If Not ftp.DirectoryExists(folder_nm) Then
                ftp.CreateDirectory(folder_nm)
            End If

            ftp.Disconnect()

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try
    End Sub


End Class
