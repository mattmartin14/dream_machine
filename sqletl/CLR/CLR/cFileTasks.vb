
Imports System.Data.SqlClient
Imports System.Threading
Imports System.IO
Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Security.Principal
Public Class cFileTasks


    <SqlFunction(DataAccess:=DataAccessKind.Read)>
    Shared Function read_line_from_txt_file(ByVal file_path As String, ByVal line_nbr As Integer) As String

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing

        Try

            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            If File.Exists(file_path) Then
                Dim res As String = File.ReadLines(file_path).Skip(line_nbr - 1).Take(1).First().ToString()
                Return res
            Else
                Throw New Exception("File [" + file_path + "] does not exist")
            End If
        Catch ex As Exception

            Dim err_msg As String = ex.Message.ToString()

            'handler for bad line
            If ex.Message.ToString.ToLower().Contains("sequence contains no elements") Then
                err_msg = "Line [" + line_nbr.ToString() + "] does not exist in file [" + file_path + "]"
            End If

            Throw New Exception(err_msg)
        Finally
            revert_context(impersonatedUser)
        End Try


    End Function


    Shared Sub main(params As Dictionary(Of String, String), ByRef err_msg As String)

        Dim sub_task As String = cMain.get_edis_pval(params, "sub_task").ToLower

        Select Case sub_task
            Case "delete_files"
                delete_files(params, err_msg)
            'Case "create_dir"
            '    create_loc_dir(params, err_msg)
            'Case "delete_dir"
            '    delete_loc_dir(params, err_msg)
            Case "copy_file"
                copy_loc_file(params, err_msg)
            Case "rename_file"
                rename_loc_file(params, err_msg)
            Case "get_file_list"
                get_loc_file_list(params, err_msg)
            Case "update_file_attribute"
                update_loc_file_attribute(params, err_msg)
            Case "watch_for_file"
                watch_for_file(params, err_msg)
            Case "move_file"
                move_file(params, err_msg)
            Case "replace_content"
                replace_file_content(params, err_msg)

            Case Else
                err_msg = "Local file sub task not found"
        End Select

    End Sub

    Private Shared Sub replace_file_content(params As Dictionary(Of String, String), ByRef err_msg As String)

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing
        Dim temp_file_path As String

        Try

            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            Dim file_path As String = cMain.get_edis_pval(params, "file_path")
            Dim orig_content As String = cMain.get_edis_pval(params, "orig_string")
            Dim replacement_content As String = cMain.get_edis_pval(params, "replace_string")
            Dim backup_file_nm As String = cMain.get_edis_pval(params, "backup_file_nm")

            temp_file_path = Path.GetDirectoryName(file_path) + "\" + Guid.NewGuid.ToString() + Path.GetExtension(file_path)

            Dim backup_full_path As String = Path.GetDirectoryName(file_path) + "\" + backup_file_nm 'backup file name has the extension


            Using fs_src As New StreamReader(file_path)

                Using fs_dest As New StreamWriter(temp_file_path)

                    While fs_src.Peek >= 0
                        Dim curr_line = fs_src.ReadLine
                        curr_line = curr_line.Replace(orig_content, replacement_content)
                        fs_dest.WriteLine(curr_line)
                    End While

                End Using

            End Using

            'if we are specifying a backup
            If Not String.IsNullOrWhiteSpace(backup_file_nm) Then

                File.Replace(temp_file_path, file_path, backup_full_path)
            Else

                File.Replace(temp_file_path, file_path, Nothing)
            End If

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        Finally
            If File.Exists(temp_file_path) Then
                File.Delete(temp_file_path)
            End If
            revert_context(impersonatedUser)
        End Try
    End Sub

    Private Shared Sub watch_for_file(params As Dictionary(Of String, String), ByRef err_msg As String)

        Dim folder_path As String = cMain.get_edis_pval(params, "folder_path")
        Dim file_crit As String = cMain.get_edis_pval(params, "file_crit")
        Dim max_timeout As Integer = CInt(cMain.get_edis_pval(params, "max_timeout"))

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing

        Try

            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            Dim start As DateTime = Now()
            While True
                Try
                    Dim files = Directory.GetFiles(folder_path, file_crit)

                    If files.Length > 0 Then
                        For Each f In files

                            Using stream As New StreamReader(f.ToString())

                            End Using

                        Next

                        'Console.WriteLine("unlock successfull")
                        Exit While
                    End If

                    'exit after max timeout
                    If DateDiff(DateInterval.Second, start, Now()) > max_timeout Then
                        Exit While
                    End If

                Catch ex As Exception
                    Thread.Sleep(1000)
                End Try

            End While
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        Finally
            revert_context(impersonatedUser)
        End Try

    End Sub

    Private Shared Sub move_file(params As Dictionary(Of String, String), ByRef err_msg As String)

        Dim src_path As String = cMain.get_edis_pval(params, "src_path")
        Dim dest_dir As String = cMain.get_edis_pval(params, "dest_dir")
        Dim force As Boolean = CBool(cMain.get_edis_pval(params, "force"))
        Dim new_file_nm As String = cMain.get_edis_pval(params, "new_file_nm")

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing

        Try
            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            If Not File.Exists(src_path) Then
                err_msg = "Source file [" + src_path + "] does not exist."

                revert_context(impersonatedUser)

                Exit Sub
            End If

            If Not Directory.Exists(dest_dir) Then
                If force = False Then
                    err_msg = "Destination directory [" + dest_dir + "] does not exist."
                    revert_context(impersonatedUser)
                    Exit Sub
                Else
                    'create the directory if forced to true
                    Directory.CreateDirectory(dest_dir)
                End If

            End If

            Dim src_file_nm As String = Path.GetFileName(src_path)


            If Right(dest_dir, 1) <> "\" Then
                dest_dir += "\"
            End If

            Dim dest_path As String

            If Not String.IsNullOrWhiteSpace(new_file_nm) Then
                dest_path = dest_dir + new_file_nm
            Else
                dest_path = dest_dir + src_file_nm
            End If

            File.Move(src_path, dest_path)

        Catch ex As Exception
            err_msg = ex.Message.ToString
        Finally
            revert_context(impersonatedUser)
        End Try


    End Sub

    Private Shared Sub delete_files(params As Dictionary(Of String, String), ByRef err_msg As String)


        Dim folder_path As String = cMain.get_edis_pval(params, "folder_path")
        Dim file_crit As String = cMain.get_edis_pval(params, "file_crit")
        Dim file_list As String = cMain.get_edis_pval(params, "file_list_usr")

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing

        Try
            If Right(folder_path, 1) <> "\" Then folder_path = folder_path + "\"

            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            'does path exist?
            If Not Directory.Exists(folder_path) Then
                err_msg = "Directory [" + folder_path + "] does not exist"
                revert_context(impersonatedUser)
                Exit Sub
            End If

            Dim file_list_delim As String = ""

            Dim dir_info As New DirectoryInfo(folder_path)

            'if using a file list
            If Trim(file_list) <> "" Then
                Dim files() = file_list.Split(";")

                For Each file_nm In files
                    Dim fPath As String = folder_path + file_nm
                    If File.Exists(fPath) Then
                        File.Delete(fPath)
                    End If
                Next

            Else
                Dim files As FileInfo() = dir_info.GetFiles(file_crit)
                For Each file In files
                    'remove readonly to delete
                    If file.IsReadOnly Then
                        file.IsReadOnly = False
                    End If
                    file.Delete()
                Next
            End If

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        Finally
            revert_context(impersonatedUser)
        End Try

    End Sub

    <SqlProcedure>
    Public Shared Sub create_loc_dir(folder_path As SqlString)

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing
        Dim err_msg As String = Nothing
        Dim outcome As String = "SUCCESS"
        Dim exec_id As String = cMain.create_exec_id()
        Dim task_nm As String = "FILE_TASK - CREATE_DIR"
        'Dim diagnostics_msg As String = "start" + vbCrLf

        Try
            'log start
            Using cn As New SqlConnection("context connection = true")
                cn.Open()
                Dim sql As String =
                    "SET NOCOUNT ON; 
                        INSERT EDIS.etl_audit 
                             (exec_id, task_action, task_start_ts, usr_login_id, ip_adrs, usr_machine_nm, local_dir) 
                            VALUES (@exec_id, @task_nm, getdate(), SUSER_NAME(), cast(CONNECTIONPROPERTY('client_net_address') as varchar(50))
                            , HOST_NAME(), @folder_path)
                        OPTION (MAXDOP 1)
                    "
                Using cmd As New SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@exec_id", exec_id)
                    cmd.Parameters.AddWithValue("@task_nm", task_nm)
                    cmd.Parameters.AddWithValue("@folder_path", folder_path.ToString())
                    SqlContext.Pipe.ExecuteAndSend(cmd)
                End Using
            End Using

            cMain.impersonate_user(clientID, impersonatedUser)

            If Not Directory.Exists(folder_path.ToString()) Then
                Directory.CreateDirectory(folder_path.ToString())
            Else
                err_msg = "Directory path [" + folder_path.ToString() + "] already exists."
                outcome = "FAILURE"
            End If

        Catch ex As Exception
            err_msg = ex.Message.ToString
            outcome = "FAILURE"
        Finally
            cMain.undo_impersonation(impersonatedUser)

            cMain.log_task_finish(exec_id, outcome, err_msg)

            'send error back to client
            If err_msg IsNot Nothing Then
                cMain.raiserror(err_msg, exec_id, task_nm)
            End If

            'SqlContext.Pipe.Send(diagnostics_msg)

        End Try

    End Sub

    'Private Shared Sub create_loc_dir(params As Dictionary(Of String, String), ByRef err_msg As String)

    '    Dim clientID As WindowsIdentity = Nothing
    '    Dim impersonatedUser As WindowsImpersonationContext = Nothing

    '    Try

    '        Dim folder_path As String = cMain.get_edis_pval(params, "folder_path")

    '        clientID = SqlContext.WindowsIdentity
    '        impersonatedUser = clientID.Impersonate()

    '        If Directory.Exists(folder_path) Then
    '            err_msg = "Directory path [" + folder_path + "] already exists"

    '            If impersonatedUser IsNot Nothing Then
    '                impersonatedUser.Undo()
    '            End If

    '            Exit Sub
    '        Else
    '            Directory.CreateDirectory(folder_path)
    '        End If

    '    Catch ex As Exception
    '        err_msg = ex.Message.ToString
    '    Finally
    '        revert_context(impersonatedUser)
    '    End Try

    'End Sub

    Shared Sub revert_context(ByRef impersonatedUser As WindowsImpersonationContext)
        If impersonatedUser IsNot Nothing Then
            impersonatedUser.Undo()
        End If
    End Sub
    'OLD
    'Private Shared Sub delete_loc_dir(params As Dictionary(Of String, String), ByRef err_msg As String)

    '    Dim clientID As WindowsIdentity = Nothing
    '    Dim impersonatedUser As WindowsImpersonationContext = Nothing

    '    Try
    '        Dim folder_path As String = cMain.get_edis_pval(params, "folder_path")

    '        clientID = SqlContext.WindowsIdentity
    '        impersonatedUser = clientID.Impersonate()

    '        If Not Directory.Exists(folder_path) Then
    '            err_msg = "Directory path [" + folder_path + "] does not exist"

    '            revert_context(impersonatedUser)
    '            Exit Sub
    '        Else
    '            Directory.Delete(folder_path, True)
    '        End If

    '    Catch ex As Exception
    '        err_msg = ex.Message.ToString
    '    Finally
    '        revert_context(impersonatedUser)
    '    End Try

    'End Sub

    'this direct execution model is WAYYYY FASTER than through the entry point....
    <SqlProcedure>
    Public Shared Sub delete_loc_dir(folder_path As SqlString)

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing
        Dim err_msg As String = Nothing
        Dim outcome As String = "SUCCESS"
        Dim exec_id As String = cMain.create_exec_id()
        Dim task_nm As String = "FILE_TASK - DELETE_DIR"
        'Dim diagnostics_msg As String = "start" + vbCrLf

        Try
            'log start
            Using cn As New SqlConnection("context connection = true")
                cn.Open()
                'cn.ChangeDatabase("SSISDB")
                Dim sql As String =
                    "SET NOCOUNT ON; 
                        INSERT EDIS.etl_audit 
                             (exec_id, task_action, task_start_ts, usr_login_id, ip_adrs, usr_machine_nm, local_dir) 
                            VALUES (@exec_id, @task_nm, getdate(), SUSER_NAME(), cast(CONNECTIONPROPERTY('client_net_address') as varchar(50))
                            , HOST_NAME(), @folder_path)
                        OPTION (MAXDOP 1)
                    "
                Using cmd As New SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@exec_id", exec_id)
                    cmd.Parameters.AddWithValue("@task_nm", task_nm)
                    cmd.Parameters.AddWithValue("@folder_path", folder_path.ToString())
                    SqlContext.Pipe.ExecuteAndSend(cmd)
                End Using
            End Using

            'diagnostics_msg += "starting impersonation. Current User is [" + Environment.UserName.ToString() + "]" + vbCrLf

            cMain.impersonate_user(clientID, impersonatedUser)

            'diagnostics_msg += "impersonation set. Current User is [" + Environment.UserName.ToString() + "]" + vbCrLf

            If Not Directory.Exists(folder_path.ToString()) Then
                err_msg = "Directory path [" + folder_path.ToString() + "] does not exist."
                outcome = "FAILURE"
            Else
                Directory.Delete(folder_path.ToString(), True)
                'diagnostics_msg += "folder deleted" + vbCrLf
            End If

        Catch ex As Exception
            err_msg = ex.Message.ToString
            outcome = "FAILURE"
        Finally
            cMain.undo_impersonation(impersonatedUser)

            cMain.log_task_finish(exec_id, outcome, err_msg)

            'send error back to client
            If err_msg IsNot Nothing Then
                cMain.raiserror(err_msg, exec_id, task_nm)
            End If

            'SqlContext.Pipe.Send(diagnostics_msg)

        End Try

    End Sub

    Private Shared Sub rename_loc_file(params As Dictionary(Of String, String), ByRef err_msg As String)

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing

        Try
            Dim curr_file_path As String = cMain.get_edis_pval(params, "curr_file_path")
            Dim new_file_path As String = cMain.get_edis_pval(params, "new_file_path")

            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            If File.Exists(curr_file_path) Then
                File.Move(curr_file_path, new_file_path)
            End If


        Catch ex As Exception
            err_msg = ex.Message.ToString
        Finally
            revert_context(impersonatedUser)
        End Try

    End Sub

    Private Shared Sub update_loc_file_attribute(params As Dictionary(Of String, String), ByRef err_msg As String)

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing

        Try
            Dim file_path As String = cMain.get_edis_pval(params, "file_path")
            Dim attribute_name As String = cMain.get_edis_pval(params, "attribute_name")

            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            If Not File.Exists(file_path) Then
                err_msg = "File [" + file_path + "] does not exist."
                revert_context(impersonatedUser)
                Exit Sub
            End If

            Select Case LCase(attribute_name)
                Case "hidden"
                    File.SetAttributes(file_path, FileAttributes.Hidden)
                Case "readonly"
                    File.SetAttributes(file_path, FileAttributes.ReadOnly)
            End Select
        Catch ex As Exception
            err_msg = ex.Message.ToString
        Finally
            revert_context(impersonatedUser)
        End Try

    End Sub

    Private Shared Sub copy_loc_file(params As Dictionary(Of String, String), ByRef err_msg As String)

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing

        Try
            Dim file_path As String = cMain.get_edis_pval(params, "file_path")
            Dim target_folder As String = cMain.get_edis_pval(params, "folder_path")

            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            If Right(target_folder, 1) <> "\" Then target_folder += "\"

            'does the source file exist?
            If Not File.Exists(file_path) Then
                err_msg = "Source file [" + file_path + "] does not exist"

                revert_context(impersonatedUser)

                Exit Sub
            End If

            'does the target folder exist?
            If Not Directory.Exists(target_folder) Then
                err_msg = "Directory path [" + target_folder + "] does not exist"

                revert_context(impersonatedUser)

                Exit Sub
            End If


            'copy
            Dim new_file_path As String = target_folder + Path.GetFileName(file_path)
            File.Copy(file_path, new_file_path, True)


        Catch ex As Exception
            err_msg = ex.Message.ToString
        Finally
            revert_context(impersonatedUser)
        End Try

    End Sub



    '****************************************************************************************************************
    'Table Valuedd Function to get files

    Private Class file_props
        Public filePath As SqlString
        Public dirPath As SqlString
        Public fileNm As SqlString
        Public Extn As SqlString
        Public fileNmWoExtn As SqlString
        Public rootPath As SqlString
        Public crtTs As SqlDateTime
        Public crtTsUtc As SqlDateTime
        Public lastWriteTs As SqlDateTime
        Public lastWriteTsUtc As SqlDateTime

        'Public lastModTs As SqlDateTime
        Public isReadOnly As SqlBoolean
        Public fileSize As SqlInt64

        Public Sub New(file_path As SqlString, dir_path As SqlString, file_nm As String, ext As SqlString,
                       file_nm_wo_ext As SqlString, root_path As SqlString, crt_ts As SqlDateTime, crt_ts_utc As SqlDateTime, last_write_ts As SqlDateTime,
                       last_write_ts_utc As SqlDateTime, is_read_only As SqlBoolean, file_size As SqlInt64)

            filePath = file_path
            dirPath = dir_path
            fileNm = file_nm
            Extn = ext
            fileNmWoExtn = file_nm_wo_ext
            rootPath = root_path
            crtTs = crt_ts
            crtTsUtc = crt_ts_utc
            lastWriteTs = last_write_ts
            lastWriteTsUtc = last_write_ts_utc
            isReadOnly = is_read_only
            fileSize = file_size

        End Sub

    End Class

    'SQL CLR Table Valued Function needs to return IEnumrable

    'Note: For impersonation to work on sql functions, you need to add DataAccess:=DataAccessKind.Read to the header

    <SqlFunction(Name:="get_files", FillRowMethodName:="get_files_fillRow", DataAccess:=DataAccessKind.Read,
                 TableDefinition:="file_path nvarchar(1000), dir_path nvarchar(1000), file_nm nvarchar(1000), ext nvarchar(250)
                 , file_nm_wo_ext nvarchar(1000), root_path nvarchar(250), crt_ts datetime, crt_ts_utc datetime
                 , last_write_ts datetime, last_write_ts_utc datetime, is_read_only bit, file_size bigint")>
    Public Shared Function get_files(src_dir As String, filter As String) As IEnumerable

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing

        Try

            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            If String.IsNullOrWhiteSpace(filter) Then
                filter = "*"
            End If

            Dim file_coll As New ArrayList()

            If Not Directory.Exists(src_dir) Then
                Throw New Exception("Directory Path [" + src_dir + "] does not exist.")
            End If

            Dim dir_info As New DirectoryInfo(src_dir)

            Dim files As FileInfo() = dir_info.GetFiles(filter)

            For Each f As FileInfo In files

                'it was including .xlsx when the filter was "*.xls"
                ' the get files thing above doesnt filter the way Like would
                If f.Name Like filter Then

                    Dim file_path As String = f.FullName
                    Dim dir_path As String = f.DirectoryName
                    Dim file_nm As String = f.Name
                    Dim ext As String = f.Extension
                    Dim file_nm_wo_ext As String = Path.GetFileNameWithoutExtension(f.FullName).ToString
                    Dim root_path As String = Path.GetPathRoot(f.FullName).ToString
                    Dim crt_ts As DateTime = f.CreationTime
                    Dim crt_ts_utc As SqlString = f.CreationTimeUtc
                    Dim last_write_ts As DateTime = f.LastWriteTime
                    Dim last_write_ts_utc As SqlString = f.LastWriteTimeUtc
                    Dim is_read_only As Boolean = f.IsReadOnly
                    Dim file_size As Integer = f.Length


                    file_coll.Add(New file_props(file_path, dir_path, file_nm, ext, file_nm_wo_ext, root_path, crt_ts, crt_ts_utc, last_write_ts, last_write_ts_utc, is_read_only, file_size))
                End If


            Next

            Return file_coll
        Catch ex As Exception
            Throw New Exception(ex.Message.ToString())
        Finally
            revert_context(impersonatedUser)
        End Try

    End Function

    'Fill row method to load the files into the TVF
    Public Shared Sub get_files_fillRow(file_props_obj As Object, ByRef file_path As SqlString, ByRef dir_path As SqlString, ByRef file_nm As String, ByRef ext As SqlString,
                                   ByRef file_nm_wo_ext As SqlString, ByRef root_path As SqlString,
                                        ByRef crt_ts As SqlDateTime, ByRef crt_ts_utc As SqlDateTime, ByRef last_write_ts As SqlDateTime, ByRef last_write_ts_utc As SqlDateTime,
                                        ByRef is_read_only As SqlBoolean, ByRef file_size As SqlInt64)
        'I'm using here the FileProperties class defined above
        Dim fprops As file_props = DirectCast(file_props_obj, file_props)

        file_path = fprops.filePath
        dir_path = fprops.dirPath
        file_nm = fprops.fileNm
        ext = fprops.Extn
        file_nm_wo_ext = fprops.fileNmWoExtn
        root_path = fprops.rootPath
        crt_ts = fprops.crtTs
        crt_ts_utc = fprops.crtTsUtc
        last_write_ts = fprops.lastWriteTs
        last_write_ts_utc = fprops.lastWriteTsUtc
        is_read_only = fprops.isReadOnly
        file_size = fprops.fileSize

    End Sub

    '================================================================================================================================================
    '================================================================================================================================================
    'Deprecated Procs


    'this is deprecated....no more updates to this one. Send users to get_files table valued function instead
    Private Shared Sub get_loc_file_list(params As Dictionary(Of String, String), ByRef err_msg As String)

        Dim clientID As WindowsIdentity = Nothing
        Dim impersonatedUser As WindowsImpersonationContext = Nothing

        Try

            Dim folder_path As String = cMain.get_edis_pval(params, "folder_path")
            Dim file_crit As String = cMain.get_edis_pval(params, "file_crit")
            Dim show_details As Boolean = CBool(cMain.get_edis_pval(params, "show_details"))
            Dim tmp_tbl_nm As String = cMain.get_edis_pval(params, "tmp_tbl_nm")

            clientID = SqlContext.WindowsIdentity
            impersonatedUser = clientID.Impersonate()

            Dim dir_info As New DirectoryInfo(folder_path)
            Dim files As FileInfo() = dir_info.GetFiles(file_crit)

            revert_context(impersonatedUser)

            'if no files, we are done
            If files Is Nothing Then Exit Sub


            Dim sql_str As String
            If show_details Then
                sql_str = "insert [" + tmp_tbl_nm + "] values (@file_nm, @full_nm, @ext, @crt_ts, @last_axs_ts, @is_read_only, @file_size)"
            Else
                sql_str = "insert [" + tmp_tbl_nm + "] values (@file_nm)"
            End If

            Using cn As New SqlConnection("context connection = true")

                cn.Open()

                For Each File As FileInfo In files
                    Using cmd As New SqlCommand(sql_str, cn)
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
            err_msg = ex.Message.ToString
        Finally
            revert_context(impersonatedUser)
        End Try

    End Sub

End Class
