Imports Microsoft.SharePoint.Client
Imports sp = Microsoft.SharePoint.Client
Imports System.Security
Imports System.Net
Imports System.Data.SqlClient
Imports System.Text.RegularExpressions
Imports System.IO

Class cSharePoint

    Private Shared sharepoint_err_msg As String = ""

    'Initialize Params
    Private Shared sharepoint_url As String = EDIS.get_edis_pval("sharepoint_url")
    Private Shared svc_uid As String = EDIS.get_edis_pval("svc_uid")
    Private Shared svc_pwd As String = EDIS.get_edis_pval("svc_pwd")
    Private Shared sharepoint_version As String = EDIS.get_edis_pval("sharepoint_version")
    Private Shared sharepoint_list_nm As String = EDIS.get_edis_pval("sharepoint_list_nm")
    Private Shared sharepoint_doc_lib_nm As String = EDIS.get_edis_pval("sharepoint_doc_lib_nm")
    Private Shared mssql_inst As String = EDIS.get_edis_pval("EDIS_MSSQL_INST")
    Private Shared file_crit As String = EDIS.get_edis_pval("file_crit")
    Private Shared local_folder_path As String = EDIS.get_edis_pval("local_folder_path")
    Private Shared output_tbl_nm As String = EDIS.get_edis_pval("output_tbl_nm")
    Private Shared batch_size As String = EDIS.get_edis_pval("batch_size")
    Private Shared src_qry As String = EDIS.get_edis_pval("src_qry")


    Shared Sub sharepoint_main(ByRef err_msg As String)

        Select Case LCase(EDIS.get_edis_pval("sub_task"))
            Case "read_sharepoint_list"
                read_sharepoint_list()
            Case "write_to_sharepoint_list"
                write_to_sharepoint_list()
            Case "purge_sharepoint_list"
                purge_sharepoint_list()
            Case "get_sharepoint_list_metadata"
                get_sharepoint_list_metadata()
            Case "upload_files_to_sharepoint"
                upload_files_to_sharepoint()
            Case "download_files_from_sharepoint"
                download_files_from_sharepoint()
            Case "get_sharepoint_doc_lib_file_list"
                get_sharepoint_doc_lib_file_list()
        End Select

        err_msg = sharepoint_err_msg

    End Sub

    Private Shared Sub get_sharepoint_doc_lib_file_list()

        Try

            Dim ctx As sp.ClientContext = New sp.ClientContext(sharepoint_url)

            login_to_sharepoint(ctx)

            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(sharepoint_doc_lib_nm)

            Dim doc_lib_items As ListItemCollection = oList.GetItems(New CamlQuery())

            ctx.Load(doc_lib_items, Function(items) _
                                        items.Include(Function(item) item.File)
                                    )
            ctx.ExecuteQuery()

            Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = true")
                cn.Open()

                'CREATE TABLE ##tmp_123 (file_nm nvarchar(4000), file_size_kb bigint, crt_ts datetime2, last_mod_ts datetime2)

                Dim sql_cmd As String = "INSERT INTO [" + output_tbl_nm + "] (file_nm, file_size_kb, crt_ts, last_mod_ts) "
                sql_cmd += "VALUES (@file_nm, @file_size_kb, @crt_ts, @last_mod_ts)"

                For Each itm As sp.ListItem In doc_lib_items
                    If itm.File.Name.ToLower Like file_crit.ToLower Then
                        Using cmd As New SqlClient.SqlCommand(sql_cmd, cn)
                            With cmd.Parameters
                                .AddWithValue("@file_nm", itm.File.Name)
                                .AddWithValue("@file_size_kb", itm.File.Length / 1024)
                                .AddWithValue("@crt_ts", itm.File.TimeCreated)
                                .AddWithValue("@last_mod_ts", itm.File.TimeLastModified)
                            End With
                            cmd.ExecuteNonQuery()
                        End Using

                    End If
                Next

            End Using

        Catch ex As Exception
            sharepoint_err_msg = ex.Message.ToString
        End Try
    End Sub

    Private Shared Sub download_files_from_sharepoint()
        Try

            Dim ctx As sp.ClientContext = New sp.ClientContext(sharepoint_url)

            login_to_sharepoint(ctx)

            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(sharepoint_doc_lib_nm)

            Dim doc_lib_items As ListItemCollection = oList.GetItems(New CamlQuery())

            ctx.Load(doc_lib_items, Function(items) _
                                        items.Include(Function(item) item.File)
                                    )
            ctx.ExecuteQuery()

            'add incase user did not supply folder path with backslash on end so that downloads don't mess up
            If Right(local_folder_path, 1) <> "\" Then local_folder_path += "\"

            For Each itm As sp.ListItem In doc_lib_items

                'if we have a match, download
                If itm.File.Name.ToLower Like file_crit.ToLower Then
                    Dim fileInformation = sp.File.OpenBinaryDirect(ctx, itm.File.ServerRelativeUrl)

                    Using fs = New FileStream(local_folder_path + itm.File.Name, FileMode.Create)
                        fileInformation.Stream.CopyTo(fs)
                    End Using

                End If

            Next

        Catch ex As Exception
            sharepoint_err_msg = ex.Message.ToString
        End Try


    End Sub

    Private Shared Sub upload_files_to_sharepoint()
        Try

            '---------------------------------------------------------------------
            'Login to SharePoint
            Dim ctx As sp.ClientContext = New sp.ClientContext(sharepoint_url)
            login_to_sharepoint(ctx)

            'get the relative URL
            Dim web = ctx.Web
            ctx.Load(web, Function(itm) itm.ServerRelativeUrl)
            ctx.ExecuteQuery()

            Dim rurl = web.ServerRelativeUrl.ToString()

            '----------------------------------------------------------------------------------------------
            'Loop through Files and Load

            Dim dir_info As New DirectoryInfo(local_folder_path)
            Dim files() As FileInfo = dir_info.GetFiles(file_crit)
            For Each f As FileInfo In files
                Using fs As FileStream = New FileStream(f.FullName, FileMode.Open)

                    'set sharepoint path string
                    Dim path_str As String = rurl + String.Format("/{0}/{1}", sharepoint_doc_lib_nm, f.Name)

                    'upload
                    Microsoft.SharePoint.Client.File.SaveBinaryDirect(ctx, path_str, fs, True)
                End Using
            Next
        Catch ex As Exception
            sharepoint_err_msg = ex.Message.ToString
        End Try

    End Sub
    Private Shared Sub get_sharepoint_list_metadata()
        Try

            Dim ctx As sp.ClientContext = New sp.ClientContext(sharepoint_url)

            login_to_sharepoint(ctx)

            'get the list object
            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(sharepoint_list_nm)

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
            Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = true")
                cn.Open()

                'CREATE TABLE STATEMENT TEMPLATE:
                'create table ##tmp123 (display_name varchar(250), internal_name varchar(250), data_type varchar(250), is_hidden bit, default_value varchar(4000), is_required bit
                '   , field_description varchar(4000)
                '   , field_formula varchar(4000)
                '   , field_class_type varchar(30)
                '   , validation_formula varchar(4000)
                '   , validation_message varchar(4000)
                ')


                Dim cmd_str As String = "INSERT [" + output_tbl_nm + "] "
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
            sharepoint_err_msg = ex.Message.ToString()
        End Try


    End Sub


    Private Shared Sub purge_sharepoint_list()

        Try

            Dim ctx As New ClientContext(sharepoint_url)

            login_to_sharepoint(ctx)

            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(sharepoint_list_nm)

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
            sharepoint_err_msg = ex.Message.ToString
        End Try

    End Sub

    Private Shared Sub write_to_sharepoint_list()

        Try

            Dim ctx As ClientContext = New ClientContext(sharepoint_url)

            login_to_sharepoint(ctx)

            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(sharepoint_list_nm)

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
            Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = true")
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
                                    sharepoint_err_msg = "Source query field [" + fld_nm + "] does not exist in sharepoint list [" + sharepoint_list_nm + "]. "
                                    sharepoint_err_msg += vbCrLf
                                    sharepoint_err_msg += "In order to write to a sharepoint list, the source query column name or alias must exist as a field in the list. "
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
            sharepoint_err_msg = ex.Message.ToString()
        End Try

    End Sub

    Private Shared Sub read_sharepoint_list()

        Try

            Dim ctx As sp.ClientContext = New sp.ClientContext(sharepoint_url)

            login_to_sharepoint(ctx)

            'get the list object
            Dim oList As sp.List = ctx.Web.Lists.GetByTitle(sharepoint_list_nm)

            'get fields
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


            Dim md As List(Of meta_data) = New List(Of meta_data)

            For Each fld As sp.Field In oFields
                Dim display_nm As String = fld.Title.ToString
                Dim internal_nm As String = fld.InternalName.ToString
                Dim sp_data_type As String = fld.TypeAsString.ToString()

                'some of the sharepoint internal names pop up twice with the same display name
                'user fields names don't repeat more than once
                If Not (md.Exists(Function(item) item.display_nm = display_nm)) Then
                    ' this filters out the ms internal sharepoint columns stuff
                    If Not (fld.SchemaXml.Contains("http://schemas.microsoft.com/sharepoint/v3")) Then
                        md.Add(New meta_data() With {.display_nm = display_nm, .internal_nm = internal_nm, .data_type = get_ss_datatype(sp_data_type), .sp_data_type = sp_data_type})
                    End If
                End If
            Next

            '======================================================================================================================
            ' Query Sharepoint List

            'Build query
            Dim qry_str As String = "<View><ViewFields>"

            For Each fcol In md
                qry_str += "<FieldRef Name = '" + fcol.internal_nm + "' />"
            Next
            qry_str += "</ViewFields></View>"

            Dim cQuery As CamlQuery = New CamlQuery
            cQuery.ViewXml = qry_str

            Dim rows As ListItemCollection = oList.GetItems(cQuery)
            ctx.Load(rows)
            ctx.ExecuteQuery()

            'Assemble the alter table statement for the sql global temp
            Dim alter_tbl_sql As String = "ALTER TABLE " + output_tbl_nm + " ADD "
            Dim insert_cols As String = "("
            Dim param_val_cols As String = "("

            Dim comma As String = ""
            For Each col In md
                alter_tbl_sql += (comma & Convert.ToString(" [")) + col.display_nm + "] " + col.data_type
                insert_cols += (comma & Convert.ToString("[")) + col.display_nm + "] "
                Dim param_val_col As String = "@" + col.display_nm.Replace(" ", "_")
                'replace colons with double underscores
                param_val_col = Replace(param_val_col, ":", "__")
                param_val_cols += comma & param_val_col
                comma = ","
            Next

            ' add bookend parenthesis
            insert_cols += ")"
            param_val_cols += ")"

            'grab the first column for the drop, update the alter statement and run it
            Using cn As New SqlConnection("server = " + mssql_inst + "; integrated security = true")
                cn.Open()
                Dim first_col_sql As String = "select COLUMN_NAME from tempdb.INFORMATION_SCHEMA.columns where TABLE_NAME = '" + output_tbl_nm + "'"

                Dim first_col As String = ""

                Using cmd As New SqlCommand(first_col_sql, cn)
                    first_col = cmd.ExecuteScalar().ToString()
                End Using

                alter_tbl_sql += (Convert.ToString("; alter table " + output_tbl_nm + " drop column [") & first_col) + "];"

                Using cmd As New SqlCommand(alter_tbl_sql, cn)
                    cmd.ExecuteNonQuery()
                End Using

                cn.Close()

            End Using

            Dim insert_statement As String = "INSERT INTO " + output_tbl_nm + " " + insert_cols + " VALUES " + param_val_cols

            'Load the data
            Using cn As New SqlConnection("server = " + mssql_inst + "; integrated security = true")
                cn.Open()

                'load the data
                For Each row As ListItem In rows
                    Using cmd As New SqlCommand(insert_statement, cn)
                        For Each col In md
                            'the rows coming out store the data as the internal column name, so we need to translate back
                            If row.FieldValues.ContainsKey(col.internal_nm) Then
                                Dim param_nm As String = "@" + col.display_nm.Replace(" ", "_")

                                'replace colons with double underscores
                                param_nm = Replace(param_nm, ":", "__")

                                If row(col.internal_nm) Is Nothing Then
                                    cmd.Parameters.AddWithValue(param_nm, DBNull.Value)
                                Else
                                    Dim param_val As String = row(col.internal_nm).ToString()

                                    'grab lookup columns
                                    If col.sp_data_type.ToLower = "lookup" Then
                                        param_val = "" 'clear the ms lookup param stuff
                                        Dim lk = DirectCast(row(col.internal_nm), FieldLookupValue)
                                        param_val = lk.LookupValue.ToString
                                    ElseIf col.sp_data_type.ToLower = "lookupmulti" Then
                                        param_val = "" 'clear the ms lookup param stuff
                                        Dim lk = DirectCast(row(col.internal_nm), FieldLookupValue())
                                        For Each itm In lk
                                            param_val += itm.LookupValue.ToString() + vbCrLf
                                        Next
                                    End If


                                    'strip out html for note columns
                                    If param_val.Contains("<p>") Then
                                        param_val = Replace(param_val, "<br>", Environment.NewLine())
                                        param_val = Regex.Replace(param_val, "<style>(.|" & vbLf & ")*?</style>", String.Empty)
                                        param_val = Regex.Replace(param_val, "<xml>(.|\n)*?</xml>", String.Empty)
                                        param_val = Regex.Replace(param_val, "<(.|\n)*?>", String.Empty)
                                    End If

                                    'after the lookup tranformations and other stuff, if we are left with an empty string set to null
                                    If param_val.ToString = "" Then
                                        cmd.Parameters.AddWithValue(param_nm, DBNull.Value)
                                    Else
                                        cmd.Parameters.AddWithValue(param_nm, param_val)
                                    End If


                                End If
                            End If
                        Next
                        cmd.ExecuteNonQuery()
                    End Using
                Next
            End Using

        Catch ex As Exception
            sharepoint_err_msg = ex.Message.ToString()
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


    Private Shared Sub login_to_sharepoint(ByRef ctx As ClientContext)
        Try
            If sharepoint_version.ToLower = "onprem" Then
                ctx.Credentials = New NetworkCredential(svc_uid, svc_pwd)
            ElseIf sharepoint_version.ToLower = "online" Then
                Dim spwd As SecureString = New SecureString
                For Each c As Char In svc_pwd.ToCharArray()
                    spwd.AppendChar(c)
                Next
                ctx.Credentials = New SharePointOnlineCredentials(svc_uid, spwd)
            Else
                sharepoint_err_msg = "SharePoint version not supported. Versions supported are 'onprem' and 'online'."
            End If
        Catch ex As Exception
            sharepoint_err_msg = ex.Message.ToString()
        End Try

    End Sub

    Private Class meta_data
        Public Property display_nm() As String
        Public Property internal_nm() As String
        Public Property data_type() As String
        Public Property sp_data_type() As String
    End Class


End Class
