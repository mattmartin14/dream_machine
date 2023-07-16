Imports Microsoft.SharePoint.Client
Imports sp = Microsoft.SharePoint.Client
Imports System.Security
Imports System.Net
Imports System.Data.SqlClient
Imports System.Text.RegularExpressions
Imports System.IO

Class cSharePoint


    Shared Sub sharepoint_main(ByRef err_msg As String)

        Select Case LCase(EDIS.get_edis_pval("sub_task"))
            Case "read_sharepoint_list"
                Dim url As String = EDIS.get_edis_pval("sharepoint_url")
                Dim list_nm As String = EDIS.get_edis_pval("sharepoint_list_nm")
                Dim uid As String = EDIS.get_edis_pval("svc_uid")
                Dim pwd As String = EDIS.get_edis_pval("svc_pwd")
                Dim show_hidden_fields As Boolean = CBool(EDIS.get_edis_pval("show_hidden_fields"))
                Dim sp_version As String = EDIS.get_edis_pval("sharepoint_version")
                Dim tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")
                read_sharepoint_list(url, list_nm, tbl_nm, uid, pwd, sp_version, show_hidden_fields, err_msg)

            Case "write_to_sharepoint_list"
                Dim url As String = EDIS.get_edis_pval("sharepoint_url")
                Dim list_nm As String = EDIS.get_edis_pval("sharepoint_list_nm")
                Dim src_qry As String = EDIS.get_edis_pval("src_qry")
                Dim batch_size As Integer = CInt(EDIS.get_edis_pval("batch_size"))
                Dim uid As String = EDIS.get_edis_pval("svc_uid")
                Dim pwd As String = EDIS.get_edis_pval("svc_pwd")
                Dim sp_version As String = EDIS.get_edis_pval("sharepoint_version")
                write_to_sharepoint_list(url, list_nm, src_qry, batch_size, uid, pwd, sp_version, err_msg)
            Case "purge_sharepoint_list"
                Dim url As String = EDIS.get_edis_pval("sharepoint_url")
                Dim list_nm As String = EDIS.get_edis_pval("sharepoint_list_nm")
                Dim batch_size As Integer = CInt(EDIS.get_edis_pval("batch_size"))
                Dim uid As String = EDIS.get_edis_pval("svc_uid")
                Dim pwd As String = EDIS.get_edis_pval("svc_pwd")
                Dim sp_version As String = EDIS.get_edis_pval("sharepoint_version")
                purge_sharepoint_list(url, list_nm, batch_size, uid, pwd, sp_version, err_msg)
            Case "get_sharepoint_list_metadata"
                Dim url As String = EDIS.get_edis_pval("sharepoint_url")
                Dim list_nm As String = EDIS.get_edis_pval("sharepoint_list_nm")
                Dim tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")
                Dim uid As String = EDIS.get_edis_pval("svc_uid")
                Dim pwd As String = EDIS.get_edis_pval("svc_pwd")
                Dim sp_version As String = EDIS.get_edis_pval("sharepoint_version")
                get_sharepoint_list_metadata(url, list_nm, tbl_nm, uid, pwd, sp_version, err_msg)
            Case "upload_files_to_sharepoint"
                Dim url As String = EDIS.get_edis_pval("sharepoint_url")
                Dim doc_lib_nm As String = EDIS.get_edis_pval("sharepoint_doc_lib_nm")
                Dim doc_lib_subfolder_nm As String = EDIS.get_edis_pval("sharepoint_doc_lib_sub_folder")
                Dim local_dir As String = EDIS.get_edis_pval("local_folder_path")
                Dim file_crit As String = EDIS.get_edis_pval("file_crit")
                Dim uid As String = EDIS.get_edis_pval("svc_uid")
                Dim pwd As String = EDIS.get_edis_pval("svc_pwd")
                Dim sp_version As String = EDIS.get_edis_pval("sharepoint_version")
                upload_files_to_sharepoint(url, doc_lib_nm, doc_lib_subfolder_nm, local_dir, file_crit, uid, pwd, sp_version, err_msg)
            Case "download_files_from_sharepoint"
                Dim url As String = EDIS.get_edis_pval("sharepoint_url")
                Dim doc_lib_nm As String = EDIS.get_edis_pval("sharepoint_doc_lib_nm")
                Dim doc_lib_subfolder_nm As String = EDIS.get_edis_pval("sharepoint_doc_lib_sub_folder")
                Dim local_dir As String = EDIS.get_edis_pval("local_folder_path")
                Dim file_crit As String = EDIS.get_edis_pval("file_crit")
                Dim uid As String = EDIS.get_edis_pval("svc_uid")
                Dim pwd As String = EDIS.get_edis_pval("svc_pwd")
                Dim sp_version As String = EDIS.get_edis_pval("sharepoint_version")
                download_files_from_sharepoint(url, doc_lib_nm, doc_lib_subfolder_nm, local_dir, file_crit, uid, pwd, sp_version, err_msg)
            Case "get_sharepoint_doc_lib_file_list"
                Dim url As String = EDIS.get_edis_pval("sharepoint_url")
                Dim doc_lib_nm As String = EDIS.get_edis_pval("sharepoint_doc_lib_nm")
                Dim tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")
                Dim file_crit As String = EDIS.get_edis_pval("file_crit")
                Dim uid As String = EDIS.get_edis_pval("svc_uid")
                Dim pwd As String = EDIS.get_edis_pval("svc_pwd")
                Dim sp_version As String = EDIS.get_edis_pval("sharepoint_version")
                get_sharepoint_doc_lib_file_list(url, doc_lib_nm, tbl_nm, file_crit, uid, pwd, sp_version, err_msg)
            Case "get_sharepoint_list_coll"
                Dim url As String = EDIS.get_edis_pval("sharepoint_url")
                Dim tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")
                Dim uid As String = EDIS.get_edis_pval("svc_uid")
                Dim pwd As String = EDIS.get_edis_pval("svc_pwd")
                Dim sp_version As String = EDIS.get_edis_pval("sharepoint_version")
                get_sharepoint_list_coll(url, tbl_nm, uid, pwd, sp_version, err_msg)
        End Select



    End Sub

    Friend Shared Sub get_sharepoint_doc_lib_file_list(url As String, doc_lib_nm As String, tbl_nm As String, file_crit As String, uid As String, pwd As String, sp_version As String, ByRef err_msg As String)

        Try

            Dim ctx As sp.ClientContext = New sp.ClientContext(url)

            login_to_sharepoint(ctx, uid, pwd, sp_version, err_msg)
            If err_msg <> "" Then Exit Sub

            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(doc_lib_nm)

            Dim cQ As New CamlQuery()
            cQ.ViewXml = "<View Scope='RecursiveAll'></View>"
            'Dim doc_lib_items As ListItemCollection = oList.GetItems(New CamlQuery())

            Dim doc_lib_items As ListItemCollection = oList.GetItems(cQ)

            'ctx.Load(doc_lib_items)
            ctx.Load(doc_lib_items, Function(items) _
                                        items.Include(Function(item) item.File, Function(item) item.Folder _
                                        , Function(item) item.FileSystemObjectType, Function(item) item.ParentList)
                                    )
            ctx.ExecuteQuery()

            Using cn As New SqlClient.SqlConnection("server = " + EDIS._mssql_inst + "; integrated security = true")
                cn.Open()

                'CREATE TABLE ##tmp_123 (file_nm nvarchar(4000), file_size_kb bigint, crt_ts datetime2, last_mod_ts datetime2)

                Dim sql_cmd As String = "INSERT INTO [" + tbl_nm + "] (file_nm, file_path, file_size_kb, crt_ts, last_mod_ts) "
                sql_cmd += "VALUES (@file_nm, @file_path, @file_size_kb, @crt_ts, @last_mod_ts)"

                For Each itm As sp.ListItem In doc_lib_items

                    If itm.FileSystemObjectType = FileSystemObjectType.File Then
                        If itm.File.Name.ToLower Like file_crit.ToLower Then

                            Using cmd As New SqlClient.SqlCommand(sql_cmd, cn)
                                With cmd.Parameters
                                    .AddWithValue("@file_nm", itm.File.Name)
                                    .AddWithValue("@file_size_kb", itm.File.Length / 1024)
                                    .AddWithValue("@crt_ts", itm.File.TimeCreated)
                                    .AddWithValue("@last_mod_ts", itm.File.TimeLastModified)
                                    .AddWithValue("@file_path", itm.File.ServerRelativeUrl.ToString)
                                End With
                                cmd.ExecuteNonQuery()
                            End Using
                        End If
                    End If



                Next

            End Using

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try
    End Sub

    Friend Shared Sub download_files_from_sharepoint(url As String, doc_lib_nm As String, doc_lib_sub_folder_path As String, local_folder_path As String,
                                                      file_crit As String, uid As String, pwd As String, sp_version As String, ByRef err_msg As String)
        Try

            'strip off slashes at end and beginning
            If Right(doc_lib_sub_folder_path, 1) = "/" Then
                doc_lib_sub_folder_path = Left(doc_lib_sub_folder_path, Len(doc_lib_sub_folder_path) - 1)
            End If

            If Left(doc_lib_sub_folder_path, 1) = "/" Then
                doc_lib_sub_folder_path = Right(doc_lib_sub_folder_path, Len(doc_lib_sub_folder_path) - 1)
            End If

            Dim ctx As sp.ClientContext = New sp.ClientContext(url)

            login_to_sharepoint(ctx, uid, pwd, sp_version, err_msg)
            If err_msg <> "" Then Exit Sub

            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(doc_lib_nm)

            Dim cQ As New CamlQuery()
            cQ.ViewXml = "<View Scope='RecursiveAll'></View>"
            'Dim doc_lib_items As ListItemCollection = oList.GetItems(New CamlQuery())

            Dim doc_lib_items As ListItemCollection = oList.GetItems(cQ)

            'ctx.Load(doc_lib_items)
            ctx.Load(doc_lib_items, Function(items) _
                                        items.Include(Function(item) item.File, Function(item) item.Folder _
                                        , Function(item) item.FileSystemObjectType, Function(item) item.ParentList)
                                    )
            ctx.ExecuteQuery()

            'add incase user did not supply folder path with backslash on end so that downloads don't mess up
            If Right(local_folder_path, 1) <> "\" Then local_folder_path += "\"

            For Each itm As sp.ListItem In doc_lib_items


                If itm.FileSystemObjectType = FileSystemObjectType.File Then
                    If itm.File.Name.ToLower Like file_crit.ToLower Then

                        'get the sub folder
                        Dim sub_folder As String = get_doc_lib_subfolder(doc_lib_nm, itm.File.ServerRelativeUrl)

                        If sub_folder.ToLower = doc_lib_sub_folder_path.ToLower Then
                            Dim fileInformation = sp.File.OpenBinaryDirect(ctx, itm.File.ServerRelativeUrl)

                            Using fs = New FileStream(local_folder_path + itm.File.Name, FileMode.Create)
                                fileInformation.Stream.CopyTo(fs)
                            End Using
                        End If


                    End If
                End If


            Next

        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try


    End Sub

    Private Shared Function get_doc_lib_subfolder(doc_lib_nm As String, url As String) As String

        Dim res As String = ""

        Dim doc_lib_pos As Integer = InStr(url, doc_lib_nm)

        Dim back_half As String = Right(url, Len(url) - (doc_lib_pos + doc_lib_nm.Length))

        Dim last_slash As Integer = back_half.LastIndexOf("/")

        If last_slash = 0 Or last_slash = -1 Then
            res = ""
        Else
            res = Left(back_half, last_slash)
        End If

        Return res

    End Function

    Friend Shared Sub upload_files_to_sharepoint(url As String, doc_lib_nm As String, doc_lib_sub_folder_path As String, local_dir As String, file_crit As String,
                                                  uid As String, pwd As String, sp_version As String, ByRef err_msg As String)
        Try

            'remove any slashes from the front and end of the sub folder path
            If Not String.IsNullOrWhiteSpace(doc_lib_sub_folder_path) Then
                If Left(doc_lib_sub_folder_path, 1) = "/" Then
                    doc_lib_sub_folder_path = Right(doc_lib_sub_folder_path, Len(doc_lib_sub_folder_path) - 1)
                End If
                If Right(doc_lib_sub_folder_path, 1) = "/" Then
                    doc_lib_sub_folder_path = Left(doc_lib_sub_folder_path, Len(doc_lib_sub_folder_path) - 1)
                End If
            End If

            '---------------------------------------------------------------------
            'Login to SharePoint
            Dim ctx As sp.ClientContext = New sp.ClientContext(url)
            login_to_sharepoint(ctx, uid, pwd, sp_version, err_msg)
            If err_msg <> "" Then Exit Sub

            'get the relative URL
            Dim web = ctx.Web
            ctx.Load(web, Function(itm) itm.ServerRelativeUrl)
            ctx.ExecuteQuery()

            Dim rurl = web.ServerRelativeUrl.ToString()

            '----------------------------------------------------------------------------------------------
            'Loop through Files and Load

            Dim dir_info As New DirectoryInfo(local_dir)
            Dim files() As FileInfo = dir_info.GetFiles(file_crit)
            For Each f As FileInfo In files
                Using fs As FileStream = New FileStream(f.FullName, FileMode.Open)

                    Dim sharePoint_folder_path As String

                    'if there is a subfolder
                    If Not String.IsNullOrWhiteSpace(doc_lib_sub_folder_path) Then
                        sharePoint_folder_path = doc_lib_nm + "/" + doc_lib_sub_folder_path
                    Else
                        sharePoint_folder_path = doc_lib_nm
                    End If

                    'set sharepoint path string
                    Dim path_str As String = rurl + String.Format("/{0}/{1}", sharePoint_folder_path, f.Name)

                    'upload
                    Microsoft.SharePoint.Client.File.SaveBinaryDirect(ctx, path_str, fs, True)
                End Using
            Next
        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub
    Friend Shared Sub get_sharepoint_list_metadata(url As String, list_nm As String, tbl_nm As String, uid As String, pwd As String, sp_version As String, ByRef err_msg As String)
        Try

            Dim ctx As sp.ClientContext = New sp.ClientContext(url)

            login_to_sharepoint(ctx, uid, pwd, sp_version, err_msg)
            If err_msg <> "" Then Exit Sub

            'get the list object
            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(list_nm)

            'get fields
            Dim oFields As sp.FieldCollection = oList.Fields

            'load title, internal name, data type, and schema
            ctx.Load(oFields,
                        Function(fields) _
                            fields.Include(Function(field__1) field__1.Title _
                                            , Function(field__1) field__1.InternalName _
                                            , Function(field__1) field__1.TypeAsString _
                                            , Function(field__1) field__1.Hidden _
                                            , Function(field__1) field__1.DefaultValue _
                                            , Function(field__1) field__1.Required _
                                            , Function(field__1) field__1.Description _
                                            , Function(field__1) field__1.ValidationFormula _
                                            , Function(field__1) field__1.ValidationMessage _
                                            , Function(Field__2) Field__2.SchemaXml) _
                            .Where(Function(field__1) field__1.Hidden = False)
            )

            ctx.ExecuteQuery()

            '-----------------------------------------------------------------------------------------
            'Open connection to server and load
            Using cn As New SqlClient.SqlConnection("server = " + EDIS._mssql_inst + "; integrated security = true")
                cn.Open()

                'CREATE TABLE STATEMENT TEMPLATE:
                'create table ##tmp123 (display_name varchar(250), internal_name varchar(250), data_type varchar(250), is_hidden bit, default_value varchar(4000), is_required bit
                '   , field_description varchar(4000)
                '   , field_formula varchar(4000)
                '   , field_class_type varchar(30)
                '   , validation_formula varchar(4000)
                '   , validation_message varchar(4000)
                ')


                Dim cmd_str As String = "INSERT [" + tbl_nm + "] "
                cmd_str += "(display_name, internal_name, data_type, is_hidden, default_value, is_required, field_description, field_formula, field_class_type, validation_formula, validation_message)"
                cmd_str += "VALUES(@display_name, @internal_name, @data_type, @is_hidden, @default_value, @is_required, @field_description, @field_formula, @field_class_type, @validation_formula, @validation_message)"

                For Each fld In oFields

                    Dim fld_formula As String = Nothing

                    'grab the forumula
                    If fld.TypeAsString.ToLower() = "calculated" Then

                        Dim cField As FieldCalculated = ctx.CastTo(Of FieldCalculated)(fld)
                        ctx.Load(cField)
                        ctx.ExecuteQuery()
                        fld_formula = cField.Formula.ToString()

                    End If

                    Using cmd As New SqlClient.SqlCommand(cmd_str, cn)
                        With cmd.Parameters
                            .AddWithValue("@display_name", fld.Title)
                            .AddWithValue("@internal_name", fld.InternalName)
                            .AddWithValue("@data_type", fld.TypeAsString)
                            .AddWithValue("@is_hidden", fld.Hidden)
                            If fld.DefaultValue IsNot Nothing Then
                                .AddWithValue("@default_value", fld.DefaultValue.ToString())
                            Else
                                .AddWithValue("@default_value", DBNull.Value)
                            End If
                            .AddWithValue("@is_required", fld.Required)
                            If (fld.Description Is Nothing Or Trim(fld.Description) = "") Then
                                .AddWithValue("@field_description", DBNull.Value)
                            Else
                                .AddWithValue("@field_description", fld.Description)
                            End If
                            If fld_formula IsNot Nothing Then
                                .AddWithValue("@field_formula", fld_formula)
                            Else
                                .AddWithValue("@field_formula", DBNull.Value)
                            End If

                            If fld.SchemaXml.Contains("http://schemas.microsoft.com/sharepoint/v3") Then
                                .AddWithValue("@field_class_type", "system_field")
                            Else
                                .AddWithValue("@field_class_type", "user_field")
                            End If
                            If fld.ValidationFormula IsNot Nothing Then
                                .AddWithValue("@validation_formula", fld.ValidationFormula.ToString)
                            Else
                                .AddWithValue("@validation_formula", DBNull.Value)
                            End If
                            If fld.ValidationMessage IsNot Nothing Then
                                .AddWithValue("@validation_message", fld.ValidationMessage.ToString)
                            Else
                                .AddWithValue("@validation_message", DBNull.Value)
                            End If
                        End With
                        cmd.ExecuteNonQuery()
                    End Using
                Next
            End Using
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try


    End Sub


    Friend Shared Sub purge_sharepoint_list(url As String, list_nm As String, batch_size As Integer, uid As String, pwd As String, sp_version As String, ByRef err_msg As String)

        Try

            Dim ctx As New ClientContext(url)

            login_to_sharepoint(ctx, uid, pwd, sp_version, err_msg)
            If err_msg <> "" Then Exit Sub

            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(list_nm)

            ' get count of items and the id's
            Dim listItems As ListItemCollection = oList.GetItems(CamlQuery.CreateAllItemsQuery())
            ctx.Load(listItems, Function(items) items.Include(Function(item) item.Id))
            ctx.ExecuteQuery()
            Dim rec_cnt As Integer = listItems.Count

            ' purge list
            If rec_cnt > 0 Then

                Dim k As Integer = 1
                For rec As Integer = rec_cnt - 1 To -1 + 1 Step -1
                    listItems(rec).DeleteObject()
                    k += 1

                    If k = CInt(batch_size) OrElse rec <= 1 Then
                        'Debug.Print("purging 50 rows")
                        ctx.ExecuteQuery()
                        k = 1
                    End If

                Next
            End If
        Catch ex As Exception
            err_msg = ex.Message.ToString
        End Try

    End Sub

    Friend Shared Sub write_to_sharepoint_list(url As String, list_nm As String, src_qry As String, batch_size As Integer, uid As String, pwd As String, sp_version As String, ByRef err_msg As String)

        Try

            Dim ctx As ClientContext = New ClientContext(url)

            login_to_sharepoint(ctx, uid, pwd, sp_version, err_msg)
            If err_msg <> "" Then Exit Sub

            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(list_nm)

            Dim oFields As sp.FieldCollection = oList.Fields

            'load title, internal name, data type, and schema
            ctx.Load(oFields,
                        Function(fields) _
                            fields.Include(Function(field__1) field__1.Title _
                                            , Function(field__1) field__1.InternalName _
                                            , Function(field__1) field__1.TypeAsString _
                                            , Function(Field__2) Field__2.SchemaXml) _
                            .Where(Function(field__1) field__1.Hidden = False)
            )

            ctx.ExecuteQuery()

            Dim cols As New Dictionary(Of String, String)

            'grab columns we will load
            For Each fld As sp.Field In oFields
                Dim display_nm As String = fld.Title.ToString
                Dim internal_nm As String = fld.InternalName.ToString


                If Not cols.ContainsKey(display_nm.ToLower) Then
                    ' this filters out the ms internal sharepoint columns stuff
                    If Not (fld.SchemaXml.Contains("http://schemas.microsoft.com/sharepoint/v3")) Then
                        cols.Add(display_nm.ToLower, internal_nm)
                    End If
                End If
            Next


            'Load the data to SharePoint
            Using cn As New SqlClient.SqlConnection("server = " + EDIS._mssql_inst + "; integrated security = true")
                cn.Open()
                Dim reader As SqlDataReader

                Using cmd As New SqlCommand(src_qry, cn)
                    reader = cmd.ExecuteReader()
                    If reader.HasRows Then
                        Dim fld_cnt As Integer = reader.FieldCount

                        Dim k As Integer = 0
                        While reader.Read
                            Dim new_sp_row As ListItemCreationInformation = New ListItemCreationInformation()
                            Dim olistItem As ListItem = oList.AddItem(new_sp_row)


                            'loop through columns and add
                            For i As Integer = 0 To reader.FieldCount - 1
                                Dim fld_nm As String = reader.GetName(i)
                                Dim fld_val = reader.GetValue(i)

                                'If query field does not exist as the sharepoint display name, throw error
                                If Not cols.ContainsKey(fld_nm.ToLower) Then
                                    err_msg = "Source query field [" + fld_nm + "] does not exist in sharepoint list [" + list_nm + "]. "
                                    err_msg += vbCrLf
                                    err_msg += "In order to write to a sharepoint list, the source query column name or alias must exist as a field in the list. "
                                    cn.Close()
                                    Exit Sub
                                End If


                                'sharepoint loads with internal names
                                Dim internal_List_col_name As String = cols.Item(fld_nm.ToLower)

                                If fld_val IsNot Nothing Then
                                    olistItem(internal_List_col_name.ToString) = fld_val.ToString()
                                Else
                                    olistItem(internal_List_col_name.ToString) = ""
                                End If
                            Next

                            'add the item
                            olistItem.Update()

                            k += 1
                            If k = CInt(batch_size) Then
                                'Debug.Print(k)
                                ctx.ExecuteQuery()
                                k = 0
                            End If

                        End While

                        'if we've finished reading, but didnt hit the batch size, go one more time
                        If k > 0 Then ctx.ExecuteQuery()

                    End If
                End Using
            End Using

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

    End Sub

    Friend Shared Sub get_sharepoint_list_coll(url As String, tbl_nm As String, uid As String, pwd As String, sp_version As String, ByRef err_msg As String)
        Try
            Dim ctx As sp.ClientContext = New sp.ClientContext(url)

            login_to_sharepoint(ctx, uid, pwd, sp_version, err_msg)

            Dim wb As Web = ctx.Web
            Dim colList As ListCollection = wb.Lists

            ctx.Load(colList)
            ctx.ExecuteQuery()

            Using cn As New SqlConnection("server = " + EDIS._mssql_inst + "; integrated security = true")
                cn.Open()
                For Each olist As sp.List In colList
                    Using cmd As New SqlClient.SqlCommand("INSERT [" + tbl_nm + "] VALUES(@list_nm)", cn)
                        cmd.Parameters.AddWithValue("@list_nm", olist.Title.ToString)
                        cmd.ExecuteNonQuery()
                    End Using
                Next
            End Using

        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try
    End Sub

    Friend Shared Sub read_sharepoint_list(ByVal url As String, ByVal list_nm As String, ByVal tbl_nm As String, ByVal uid As String, ByVal pwd As String,
                                            ByVal sp_version As String, ByVal show_hidden_fields As Boolean, ByRef err_msg As String)

        log_edis_event("info", "sharepoint read list", "start")

        Dim curr_col As String = ""

        Try
            Dim tgt_schema As String = "dbo"
            Dim tgt_db As String = "tempdb"

            Dim ctx As sp.ClientContext = New sp.ClientContext(url)

            login_to_sharepoint(ctx, uid, pwd, sp_version, err_msg)
            If err_msg <> "" Then Exit Sub

            'If sharepoint_err_msg <> "" Then Exit Sub

            log_edis_event("info", "sharepoint read list", "logged in successfully")

            'get the list object
            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(list_nm)

            'get fields
            Dim oFields As sp.FieldCollection = oList.Fields


            'Dim show_hidden_fields As Boolean = CBool(EDIS.get_edis_pval("show_hidden_fields"))

            If show_hidden_fields Then
                '9/6/17 -- allow hidden fields
                ctx.Load(oFields,
                            Function(fields) _
                                fields.Include(Function(field__1) field__1.Title _
                                                , Function(field__1) field__1.InternalName _
                                                , Function(field__1) field__1.TypeAsString _
                                                , Function(Field__2) Field__2.SchemaXml)
                )


            Else
                ctx.Load(oFields,
                            Function(fields) _
                                fields.Include(Function(field__1) field__1.Title _
                                                , Function(field__1) field__1.InternalName _
                                                , Function(field__1) field__1.TypeAsString _
                                                , Function(Field__2) Field__2.SchemaXml) _
                                .Where(Function(field__1) field__1.Hidden = False)
                )
            End If

            ctx.ExecuteQuery()

            'get the list relative URL (used later for retrieving attachment names)
            ctx.Load(oList, Function(l) l.RootFolder.ServerRelativeUrl)
            ctx.ExecuteQuery()


            Dim md As List(Of meta_data) = New List(Of meta_data)

            Dim dt As New DataTable

            Dim ord_pos As Integer = 0
            For Each fld As sp.Field In oFields
                Dim display_nm As String = fld.Title.ToString
                Dim internal_nm As String = fld.InternalName.ToString
                Dim sp_data_type As String = fld.TypeAsString.ToString()

                'some of the sharepoint internal names pop up twice with the same display name
                'user fields names don't repeat more than once
                If Not (md.Exists(Function(item) item.display_nm = display_nm)) Then
                    ' this filters out the ms internal sharepoint columns stuff
                    ''Include the "title" column though as a lot of ppl rename this one
                    'If (Not (fld.SchemaXml.Contains("http://schemas.microsoft.com/sharepoint/v3")) Or internal_nm.ToLower = "title") Then
                    '    md.Add(New meta_data() With {.display_nm = display_nm, .internal_nm = internal_nm, .data_type = get_ss_datatype(sp_data_type), .sp_data_type = sp_data_type, .position = ord_pos})
                    '    ord_pos += 1
                    'End If

                    'If (Not (internal_nm.ToLower = "title")) Then
                    md.Add(New meta_data() With {.display_nm = display_nm, .internal_nm = internal_nm, .data_type = get_ss_datatype(sp_data_type), .sp_data_type = sp_data_type, .position = ord_pos})
                    ord_pos += 1
                    'End If

                    dt.Columns.Add(display_nm, GetType(String))

                End If

                Dim items As String = String.Format("SharePoint column metadata loaded with following details: column friendly name {0}, internal name {1}, sp_data_type {2}", display_nm, internal_nm, sp_data_type.ToString)

                log_edis_event("info", "sharepoint list read", items)

                'Console.WriteLine(items)

            Next

            'add edis enhancement columns to the data table
            dt.Columns.Add("attach_file_names", GetType(String))

            '---------------------------------------------------------------------------------
            ' Get time zone info for system timestamps

            Dim tz_settings = ctx.Web.RegionalSettings.TimeZone

            ctx.Load(tz_settings)
            ctx.ExecuteQuery()

            Dim fTzNm = tz_settings.Description.Replace("and", "&")
            Dim tz_info = TimeZoneInfo.GetSystemTimeZones().FirstOrDefault(Function(tz) tz.DisplayName = fTzNm)


            '======================================================================================================================
            ' Query Sharepoint List

            'NOTE: SHAREPOINT HANDLES A MAX of 1K ROWS PER FETCH...NEED TO LOOP

            ''Get Max ID
            'Dim max_id_qry As String = " < View <> Query <> Where <> IsNotNull <> FieldRef Name = 'ID'/></IsNotNull></Where>"
            'max_id_qry += "<OrderBy><FieldRef Name='ID' Ascending='False' /></OrderBy></Query><RowLimit>1</RowLimit></View>"

            'Dim maxCQuery As CamlQuery = New CamlQuery
            'maxCQuery.ViewXml = max_id_qry

            'Dim res As ListItemCollection = oList.GetItems(maxCQuery)
            'ctx.Load(res)
            'ctx.ExecuteQuery()

            'If res.Count = 0 Then
            '    err_msg = "SharePoint List [" + list_nm + "] does not have any rows"
            '    Exit Sub
            'End If

            'Dim max_id As String

            'For Each row As ListItem In res
            '    max_id = row("ID").ToString()
            'Next

            'log_edis_event("info", "sharepoint read list", "max ID = " + max_id.ToString)

            '--------------------------------------------------------------------------------------------------------------------------------
            'Loop through ID's in batches of 5K

            Dim batch_size As Integer = 1000

            Dim batch_row_cnt As Integer = 0

            Dim is_first_load_call As Boolean = True
            Dim src_cols As New List(Of cDataTypeParser.src_column_metadata)
            Dim tgt_col_list As New List(Of String)


            Dim field_view_str As String = "<View>"
            field_view_str += "<RowLimit>5000</RowLimit>"
            field_view_str += "<ViewFields>"
            For Each fcol In md
                field_view_str += "<FieldRef Name = '" + fcol.internal_nm + "' />"
            Next

            field_view_str += "</ViewFields>"

            field_view_str += "</View>"

            log_edis_event("info", "SharePoint List Read Query", field_view_str)

            Dim itemPositionPager As ListItemCollectionPosition = Nothing

            While True

                '2018-01-10; changed for start location from 0 to 1 (sharepoint is at base 1 i think)
                'For i = 1 To CInt(max_id)

                'log_edis_event("info", "sharepoint read list", "curr id min = " + i.ToString)


                'Set Min/max bounds
                'Dim min_id As Integer = i
                'Dim curr_max_id As Integer = i + 4999

                'Update SharePoint Query to filter for this
                'NOte: according to sharepoint experts, greater than or equal and less than or equal doesnt work
                ' need to use greater than or less than
                'Dim sp_list_query As String = "<Query>"
                'sp_list_query += "<Where><And>"
                'sp_list_query += "<Gt><FieldRef Name='ID'></FieldRef><Value Type='Number'>" + (min_id - 1).ToString + "</Value></Gt>"
                'sp_list_query += "<Lt><FieldRef Name='ID'></FieldRef><Value Type='Number'>" + (curr_max_id + 1).ToString + "</Value></Lt>"

                'looks like the theory of only 5k rows is not true...just unload the whole thing
                'sp_list_query += "<Lt><FieldRef Name='ID'></FieldRef><Value Type='Number'>" + (max_id).ToString + "</Value></Lt>"
                'sp_list_query += "</And></Where>"

                ' sp_list_query += field_view_str

                'added 1/13/18 to handle over 5k rows
                ' sp_list_query += "<OrderBy><FieldRef Name='ID' Ascending='True'/></OrderBy>"

                'sp_list_query += "</Query>"

                'log_edis_event("info", "SharePoint List Read Query", sp_list_query)



                Dim listQuery As CamlQuery = New CamlQuery
                listQuery.ListItemCollectionPosition = itemPositionPager
                listQuery.ViewXml = field_view_str
                'listQuery.ViewXml = sp_list_query



                Dim rows As ListItemCollection = oList.GetItems(listQuery)
                ctx.Load(rows)
                ctx.ExecuteQuery()

                log_edis_event("info", "sharepoint list read", "query issued to retrieve rows")

                'update page position
                itemPositionPager = rows.ListItemCollectionPosition

                If rows.Count = 0 Then
                    err_msg = "Sharepoint list [" + list_nm + "] does not have any rows"
                    Exit Sub
                End If


                For Each row As ListItem In rows

                    Dim dr As DataRow = dt.NewRow

                    For Each col In md

                        Dim orig_val As String = ""

                        'the rows coming out store the data as the internal column name, so we need to translate back
                        If row.FieldValues.ContainsKey(col.internal_nm) Then

                            curr_col = col.display_nm

                            If row(col.internal_nm) Is Nothing Then

                                dr(col.display_nm) = DBNull.Value

                            Else
                                Dim param_val As String = row(col.internal_nm).ToString()

                                orig_val = param_val


                                'grab lookup columns
                                If col.sp_data_type.ToLower = "lookup" Then


                                    param_val = "" 'clear the ms lookup param stuff
                                    Dim lk
                                    Try
                                        lk = DirectCast(row(col.internal_nm), FieldLookupValue)
                                        If lk.LookupValue IsNot Nothing Then
                                            param_val = lk.LookupValue.ToString
                                        Else
                                            param_val = ""
                                        End If
                                    Catch ex As Exception
                                        'default to the original value
                                        param_val = orig_val
                                        'param_val = ""
                                    End Try

                                ElseIf col.sp_data_type.ToLower = "guid" Then
                                    Dim g
                                    Try
                                        g = DirectCast(row(col.internal_nm), FieldGuid)
                                        If g IsNot Nothing Then
                                            param_val = g.ToString()
                                        End If
                                    Catch ex As Exception
                                        param_val = orig_val
                                    End Try


                                ElseIf col.sp_data_type.ToLower = "lookupmulti" Then
                                    param_val = "" 'clear the ms lookup param stuff
                                    Dim lk = DirectCast(row(col.internal_nm), FieldLookupValue())
                                    For Each itm In lk
                                        If itm.LookupValue IsNot Nothing Then
                                            param_val += itm.LookupValue.ToString() + vbCrLf
                                        End If
                                    Next

                                    'the datetime for created/modified is in GMT time...calibrate to local time zone
                                ElseIf col.sp_data_type.ToLower = "datetime" Then
                                    If (col.internal_nm.ToLower = "created" Or col.internal_nm.ToLower = "modified") Then
                                        param_val = TimeZoneInfo.ConvertTimeFromUtc(row(col.internal_nm), tz_info)
                                    End If


                                ElseIf col.sp_data_type.ToLower = "user" Then
                                    Dim user_nm = DirectCast(row(col.internal_nm), FieldUserValue)

                                    If user_nm IsNot Nothing Then
                                        param_val = user_nm.LookupValue.ToString()
                                    Else
                                        param_val = ""
                                    End If

                                    'ElseIf col.sp_data_type.ToLower = "computed" Then
                                    '    Dim cVal = DirectCast(row(col.internal_nm), FieldComputed)

                                    '    cVal.ToString()

                                    'for the attachments column, if there are attachments add them to the attach_file_names col
                                End If


                                'strip out html for note columns
                                If param_val.Contains("<p>") Or param_val.Contains("</font>") Then
                                    param_val = Replace(param_val, "<br>", Environment.NewLine())
                                    param_val = Regex.Replace(param_val, "<style>(.|" & vbLf & ")*?</style>", String.Empty)
                                    param_val = Regex.Replace(param_val, "<xml>(.|\n)*?</xml>", String.Empty)
                                    param_val = Regex.Replace(param_val, "<(.|\n)*?>", String.Empty)
                                End If

                                'after the lookup tranformations and other stuff, if we are left with an empty string set to null
                                If param_val.ToString = "" Then

                                    dr(col.display_nm) = DBNull.Value
                                Else
                                    dr(col.display_nm) = param_val
                                End If

                            End If

                            '------------------------------------------------------
                            'EDIS enhanced columns
                            'attachment file list

                            If col.internal_nm.ToLower = "attachments" Then

                                Try
                                    Dim file_name_list As String = ""

                                    'query the attachment name if applicable
                                    If orig_val.ToLower = "true" Then

                                        log_edis_event("info", "sharpoint list read", "attachment collection loading")

                                        Dim fdl As Folder = ctx.Web.GetFolderByServerRelativeUrl(oList.RootFolder.ServerRelativeUrl + "/Attachments/" + row.Id.ToString)

                                        Dim attach_files As FileCollection = fdl.Files
                                        ctx.Load(attach_files, Function(afs) afs.Include(Function(f) f.Name, Function(f) f.ServerRelativeUrl))
                                        ctx.ExecuteQuery()

                                        For Each itm In attach_files
                                            file_name_list += itm.Name.ToString() + "; "
                                        Next

                                    End If

                                    If file_name_list.ToString = "" Then

                                        dr("attach_file_names") = DBNull.Value
                                    Else
                                        dr("attach_file_names") = file_name_list
                                    End If
                                Catch ex As Exception

                                End Try


                            End If



                        End If

                    Next

                    'add the row
                    dt.Rows.Add(dr)
                    batch_row_cnt += 1


                    If batch_row_cnt >= batch_size Then

                        'is this the first call to the load?
                        If is_first_load_call Then
                            is_first_load_call = False
                            'get the source columns
                            src_cols = cDataTypeParser.parse_source_data_types(dt, 10000, err_msg)
                            If err_msg <> "" Then Exit Sub
                            'If crt_dest_tbl Then

                            cDT_Tasks.create_dest_tbl(EDIS._mssql_inst, tgt_db, tgt_schema, tbl_nm, src_cols, True, err_msg)
                            If err_msg <> "" Then Exit Sub
                            'End If
                            tgt_col_list = cDT_Tasks.get_tgt_tbl_cols(EDIS._mssql_inst, tgt_db, tgt_schema, tbl_nm, err_msg)
                            If err_msg <> "" Then Exit Sub
                        End If


                        'Console.WriteLine("bulk loading batch {0}", batch_id)
                        cDT_Tasks.bulk_insert(EDIS._mssql_inst, tgt_db, tgt_schema, tbl_nm, tgt_col_list, dt, batch_size, False, "", "", err_msg)
                        If err_msg <> "" Then Exit Sub
                        dt.Clear()
                        'batch_id += 1
                        batch_row_cnt = 0
                    End If

                Next

                'edited to just exit the loop completely...it gets it all in 1 shot. original assumption of 1k rows was incorrect
                'Exit For

                'Bump ID val
                'i = curr_max_id

                'log_edis_event("info", "SharePoint read list", "read through " + i.ToString + " rows")




                'are we done paging results?
                If itemPositionPager Is Nothing Then
                    Exit While
                End If

            End While

            'Next


            'Load last batch if we didnt
            If batch_row_cnt > 0 Then

                'is this the first call to the load?
                If is_first_load_call Then
                    is_first_load_call = False
                    'get the source columns
                    src_cols = cDataTypeParser.parse_source_data_types(dt, 10000, err_msg)
                    If err_msg <> "" Then Exit Sub
                    'If crt_dest_tbl Then
                    cDT_Tasks.create_dest_tbl(EDIS._mssql_inst, tgt_db, tgt_schema, tbl_nm, src_cols, True, err_msg)
                    If err_msg <> "" Then Exit Sub
                    ' End If
                    tgt_col_list = cDT_Tasks.get_tgt_tbl_cols(EDIS._mssql_inst, tgt_db, tgt_schema, tbl_nm, err_msg)
                    If err_msg <> "" Then Exit Sub
                End If

                'Console.WriteLine("bulk loading batch {0}", batch_id)
                cDT_Tasks.bulk_insert(EDIS._mssql_inst, tgt_db, tgt_schema, tbl_nm, tgt_col_list, dt, batch_size, False, "", "", err_msg)
                If err_msg <> "" Then Exit Sub
                dt.Clear()
            End If


        Catch ex As Exception
            err_msg = ex.Message.ToString()
            'sharepoint_err_msg += "; Column that caused error: " + curr_col



            'MsgBox(sharepoint_err_msg)
        End Try

    End Sub

    Private Shared Function get_ss_datatype(sp_datatype As String) As String
        'default if we can't peg
        Dim res As String = "nvarchar(4000)"

        Select Case sp_datatype.ToLower()
            Case "datetime"
                res = "datetime"
                Exit Select
            Case "number"
                res = "float"
                Exit Select
        End Select

        Return res
    End Function


    'Private Shared Sub login_to_sharepoint(ByRef ctx As ClientContext)
    '    Try
    '        If sharepoint_version.ToLower = "onprem" Then
    '            ctx.Credentials = New NetworkCredential(svc_uid, svc_pwd)
    '        ElseIf sharepoint_version.ToLower = "online" Then
    '            Dim spwd As SecureString = New SecureString
    '            For Each c As Char In svc_pwd.ToCharArray()
    '                spwd.AppendChar(c)
    '            Next
    '            ctx.Credentials = New SharePointOnlineCredentials(svc_uid, spwd)
    '        Else
    '            sharepoint_err_msg = "SharePoint version not supported. Versions supported are 'onprem' and 'online'."
    '        End If
    '    Catch ex As Exception
    '        sharepoint_err_msg = ex.Message.ToString()
    '    End Try

    'End Sub

    Private Shared Sub login_to_sharepoint(ByRef ctx As ClientContext, ByVal uid As String, ByVal pwd As String, ByVal sp_version As String, ByRef err_msg As String)
        Try
            If sp_version.ToLower = "onprem" Then
                ctx.Credentials = New NetworkCredential(uid, pwd)
            ElseIf sp_version.ToLower = "online" Then
                Dim spwd As SecureString = New SecureString
                For Each c As Char In pwd.ToCharArray()
                    spwd.AppendChar(c)
                Next
                ctx.Credentials = New SharePointOnlineCredentials(uid, spwd)
            Else
                err_msg = "SharePoint version not supported. Versions supported are 'onprem' and 'online'."
            End If
        Catch ex As Exception
            err_msg = ex.Message.ToString()
        End Try

    End Sub

    Private Class meta_data
        Public Property display_nm As String
        Public Property internal_nm As String
        Public Property data_type As String
        Public Property sp_data_type As String

        Public Property position As Integer

    End Class


End Class
