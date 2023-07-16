
Imports Microsoft.SqlServer.Server
Imports Microsoft.Win32
Imports Microsoft.Win32.Registry
Imports System.Data.SqlTypes
Imports System.Data.OleDb
Imports System.Data.Odbc
Imports System.IO
Imports System.Text
Public Class cSysTasks

    Public Shared Sub validate_db_conn(ByVal cn_str As SqlString, ByVal cn_type As SqlString, ByRef err_msg As SqlString)
        Try
            Select Case cn_type.ToString.ToLower()
                Case "oledb"
                    Using cn As New OleDbConnection(cn_str)
                        cn.Open()
                        cn.Close()
                    End Using
                Case "odbc"
                    Using cn As New OdbcConnection(cn_str)
                        cn.Open()
                        cn.Close()
                    End Using
            End Select
        Catch ex As Exception
            err_msg = ex.Message.ToString()
            'cMain.raiserror(err_msg)
        End Try
    End Sub

    <SqlProcedure>
    Public Shared Sub get_cn_props(ByVal service_id As SqlString)

        Try
            Dim service_type
            Dim cn_str

            'get info
            Using cn As New SqlClient.SqlConnection("context connection = true")
                cn.Open()
                Dim sql As String = "select isnull(SSISDB.edis.ifn_get_svc_config(@service_id).value('(/EDIS_SVC_CONFIG/service_type) [1]','nvarchar(250)'),'N/A')"
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@service_id", service_id)
                    service_type = cmd.ExecuteScalar()
                End Using
            End Using

            If service_type <> "DB_CONN" Then
                Throw New Exception("The Service ID provided [" + service_id + "] is not a database connection and not suitable for this function.")
            End If


            Using cn As New SqlClient.SqlConnection("context connection = true")
                cn.Open()
                Dim sql As String = "select SSISDB.edis.ifn_get_svc_config(@service_id).value('(/EDIS_SVC_CONFIG/cn_str) [1]','nvarchar(4000)')"
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@service_id", service_id)
                    cn_str = cmd.ExecuteScalar()
                End Using
            End Using

            Dim pipe As SqlPipe = SqlContext.Pipe

            Dim rec As SqlDataRecord = New SqlDataRecord(New SqlMetaData() {
                New SqlMetaData("prop_nm", SqlDbType.NVarChar, 250),
                New SqlMetaData("prop_val", SqlDbType.NVarChar, 4000)
            })


            ' pipe.Send("metadata created")

            Dim props() As String = cn_str.ToString.Split(";")

            ' pipe.Send("props split on ;")

            'Dim rec As New SqlDataRecord(cols)
            pipe.SendResultsStart(rec)

            For Each prop In props

                If prop.Contains("=") Then
                    Dim prop_nm As String = Trim(prop.Split("=")(0))
                    Dim prop_val As String = Trim(prop.Split("=")(1))


                    rec.SetSqlString(0, New SqlString(prop_nm))
                    rec.SetSqlString(1, New SqlString(prop_val))

                    pipe.SendResultsRow(rec)
                End If



            Next

            pipe.SendResultsEnd()



        Catch ex As Exception
            Throw New Exception(ex.Message.ToString())
        End Try
    End Sub

    Private Class cn_props
        Public propNm As SqlString
        Public propVal As SqlString

        Public Sub New(prop_nm As SqlString, prop_val As SqlString)
            propNm = prop_nm
            propVal = prop_val
        End Sub

    End Class

    <SqlFunction(Name:="fn_get_cn_props", FillRowMethodName:="get_cn_props_fillRow", DataAccess:=DataAccessKind.Read,
                 TableDefinition:="prop_nm nvarchar(1000), prop_val nvarchar(4000)")>
    Public Shared Function fn_get_cn_props(ByVal service_id As SqlString) As IEnumerable

        Try

            Dim service_type
            Dim cn_str

            'get info
            Using cn As New SqlClient.SqlConnection("context connection = true")
                cn.Open()
                Dim sql As String = "select SSISDB.edis.ifn_get_svc_config(@service_id).value('(/EDIS_SVC_CONFIG/service_type) [1]','nvarchar(250)')"
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@service_id", service_id)
                    service_type = cmd.ExecuteScalar()
                End Using
            End Using

            If service_type <> "DB_CONN" Then
                Throw New Exception("The Service ID provided [" + service_id + "] is not a database connection and not suitable for this function.")
            End If

            Using cn As New SqlClient.SqlConnection("context connection = true")
                cn.Open()
                Dim sql As String = "select SSISDB.edis.ifn_get_svc_config(@service_id).value('(/EDIS_SVC_CONFIG/cn_str) [1]','nvarchar(4000)')"
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    cmd.Parameters.AddWithValue("@service_id", service_id)
                    cn_str = cmd.ExecuteScalar()
                End Using
            End Using

            Dim prop_row As New ArrayList()

            Dim props() As String = cn_str.ToString.Split(";")

            For Each prop In props
                prop_row.Add(New cn_props(Trim(prop.Split("=")(0)), Trim(prop.Split("=")(1))))
            Next

            Return prop_row

        Catch ex As Exception
            Throw New Exception(ex.Message.ToString())
        End Try


    End Function

    Public Shared Sub get_cn_props_fillRow(props_obj As Object, ByRef prop_nm As SqlString, ByRef prop_val As SqlString)

        Dim cnProps As cn_props = DirectCast(props_obj, cn_props)

        prop_nm = cnProps.propNm
        prop_val = cnProps.propVal

    End Sub

    '//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    <SqlProcedure>
    Public Shared Sub check_proxy_account(proxy_nm As SqlString, service_id As SqlString, ByRef res As SqlString)

        'initialize
        res = "PROXY_VALID"

        Using cn As New SqlClient.SqlConnection("context connection = true")
            cn.Open()

            'is sql agent running?
            Dim sql As String = "SELECT ISNULL((SELECT 1 FROM MASTER.dbo.sysprocesses WHERE program_name = N'SQLAgent - Generic Refresher'),0) as is_running"
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                Dim is_agent_running = CBool(cmd.ExecuteScalar())
                If Not is_agent_running Then
                    res = "In Order to run this task, SQL Server Job Agent must be running."
                End If
            End Using
            If res <> "PROXY_VALID" Then GoTo finish

            'does the proxy exist?
            sql = "SELECT ISNULL((SELECT 1 FROM msdb.dbo.sysproxies where  name = @proxy_nm),0) as proxy_exists"
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                cmd.Parameters.AddWithValue("@proxy_nm", proxy_nm)
                Dim does_proxy_exist = CBool(cmd.ExecuteScalar())
                If Not does_proxy_exist Then
                    res = "The proxy account associated with EDIS Service ID [" + service_id + "] is missing. Please reload service ID [" + service_id + "]."
                End If
            End Using
            If res <> "PROXY_VALID" Then GoTo finish

            'is the proxy mapped to cmd exec? if not, enable it
            sql = "SELECT ISNULL((
					SELECT 1
					FROM msdb.dbo.sysproxies AS p
						INNER JOIN	msdb.dbo.sysproxysubsystem AS sub
							ON sub.proxy_id = p.proxy_id
					WHERE p.name = @proxy_nm
						AND sub.subsystem_id = 3 -- CmdExec
						-- Powershell has security signing issues
						--AND sub.subsystem_id = 12 -- powershell
				),0) as is_proxy_mapped_to_cmd_exec"
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                cmd.Parameters.AddWithValue("@proxy_nm", proxy_nm)
                Dim is_cmd_exec_mapped = CBool(cmd.ExecuteScalar())
                If Not is_cmd_exec_mapped Then
                    Dim enable_sql As String = "EXEC msdb.dbo.sp_grant_proxy_to_subsystem @proxy_name=N'" + proxy_nm + "', @subsystem_id=3"
                    Using cmd2 As New SqlClient.SqlCommand(enable_sql, cn)
                        cmd2.ExecuteNonQuery()
                    End Using
                End If
            End Using

            '--------------------------------------------------------------------------------------
            'check the credential associated with the proxy

            sql = "
                
				SELECT ISNULL((SELECT C.credential_identity
				FROM master.sys.credentials AS c
					INNER JOIN msdb.dbo.sysproxies AS px
						ON c.credential_id = px.credential_id
				WHERE px.name = @proxy_nm),'N/A') as cred_nm
"
            Dim cred_nm As String
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                cmd.Parameters.AddWithValue("@proxy_nm", proxy_nm)
                cred_nm = cmd.ExecuteScalar()
            End Using
            If cred_nm = "N/A" Then
                res = "No Credential was found associated with proxy ID [" + proxy_nm + "]. Please reload EDIS Service ID [" + service_id + "]"
                GoTo finish
            End If

            'is the credential mapped to SQL Server?
            sql = "SELECT ISNULL((SELECT 1 FROM master.sys.server_principals where name = @cred_nm),0) as is_cred_mapped_to_mssql"
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                cmd.Parameters.AddWithValue("@cred_nm", cred_nm)
                Dim is_cred_mapped As Boolean = CBool(cmd.ExecuteScalar())
                If Not is_cred_mapped Then
                    sql = "CREATE LOGIN [" + cred_nm + "] FROM WINDOWS"
                    cn.ChangeDatabase("master")
                    Using cmd2 As New SqlClient.SqlCommand(sql, cn)
                        cmd2.ExecuteNonQuery()
                    End Using
                End If
            End Using

            'is the credential mapped to SSISDB? If not, do it and add to EDIS role
            sql = "SELECT ISNULL((SELECT 1 
                    FROM SSISDB.sys.database_principals WHERE sid = (SELECT sid FROM sys.server_principals WHERE name = @cred_nm)),0) as is_mapped_to_ssisdb
"
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                cmd.Parameters.AddWithValue("@cred_nm", cred_nm)
                Dim is_mapped_to_ssisdb As Boolean = CBool(cmd.ExecuteScalar())
                If Not is_mapped_to_ssisdb Then
                    sql = "CREATE USER [" + cred_nm + "] FOR LOGIN [" + cred_nm + "]"
                    cn.ChangeDatabase("SSISDB")
                    Using cmd2 As New SqlClient.SqlCommand(sql, cn)
                        cmd2.ExecuteNonQuery()
                    End Using
                End If
            End Using

            'is cred mapped to EDIS_ROLE?
            sql = "SELECT ISNULL((SELECT 1
							FROM SSISDB.sys.database_role_members
							WHERE role_principal_id = (SELECT principal_id FROM SSISDB.sys.database_principals WHERE name = 'EDIS_Role')
								AND member_principal_id = (SELECT principal_id 
                                                           FROM SSISDB.sys.database_principals WHERE sid = (SELECT sid FROM sys.server_principals WHERE name = @cred_nm))
						),0) as is_mapped_to_edis_role"
            Using cmd As New SqlClient.SqlCommand(sql, cn)
                cmd.Parameters.AddWithValue("@cred_nm", cred_nm)
                Dim is_mapped_to_edis_role As Boolean = CBool(cmd.ExecuteScalar())
                If Not is_mapped_to_edis_role Then
                    sql = "ALTER ROLE [EDIS_Role] ADD Member [" + cred_nm + "]"
                    Using cmd2 As New SqlClient.SqlCommand(sql, cn)
                        cmd2.ExecuteNonQuery()
                    End Using
                End If
            End Using

finish:

        End Using

    End Sub

    <SqlFunction>
    Public Shared Function convert_file_to_varbinary(file_path As String) As SqlBinary

        If Not IO.File.Exists(file_path) Then
            Return Nothing
            Exit Function
        End If

        Dim res As SqlBinary

        Using fs As New FileStream(file_path, FileMode.Open)
            Dim buffer As Byte() = New Byte(fs.Length - 1) {}
            fs.Read(buffer, 0, CInt(fs.Length))
            res = buffer
            fs.Close()
        End Using

        'Dim fs As New FileStream(file_path, FileMode.Open)

        Return res

    End Function

    <SqlProcedure>
    Public Shared Sub convert_varbinary_to_file(binary_data As SqlBinary, output_file_path As String)

        If File.Exists(output_file_path) Then
            File.Delete(output_file_path)
        End If

        File.WriteAllBytes(output_file_path, binary_data)
    End Sub

    <SqlFunction>
    Public Shared Function does_file_exist(file_path As String) As Boolean

        Return IO.File.Exists(file_path)

    End Function

    <SqlFunction>
    Public Shared Function read_file_as_str(file_path As String) As String
        Dim res As String = IO.File.ReadAllText(file_path)
        Return res
    End Function

    <SqlFunction(Name:="split_string", FillRowMethodName:="fill_row_split_string", TableDefinition:="value nvarchar(max)")>
    Public Shared Function split_string(str_list As String, delim As String) As IEnumerable

        If delim.Length = 0 Then
            Return str_list
        End If

        Dim vals As String() = str_list.Split(New String() {delim}, StringSplitOptions.None)

        Dim trimmed_vals(UBound(vals)) As String

        For i = 0 To UBound(vals)
            trimmed_vals(i) = Trim(vals(i))
        Next

        Return trimmed_vals

    End Function

    Public Shared Sub fill_row_split_string(row As Object, ByRef str As SqlString)
        str = New SqlString(DirectCast(row, String))
    End Sub



    <SqlFunction>
    Public Shared Function get_ms_ace_provider() As String
        'sends back ace version in syntax "[version number];[bit wise]" i.e. "12;64" for version 12, 64 bit
        Try
            Dim ace_version As String
            Dim bit_wise As String

            Dim search_key_path As String

            Dim ret_val As String

            Dim ace_clsid As String
            Dim office_path As String

            For i As Integer = 1 To 3
                Dim ace_version_to_check As String = Choose(i, "12", "14", "16")
                search_key_path = "HKEY_CLASSES_ROOT\Microsoft.ACE.OLEDB." + ace_version_to_check + ".0\CLSID"
                Dim val As Object = My.Computer.Registry.GetValue(search_key_path, "", Nothing)
                If val IsNot Nothing Then
                    ace_clsid = val.ToString
                    ace_version = ace_version_to_check
                    Exit For
                End If
            Next

            If ace_clsid Is Nothing Then
                Return Nothing
            End If

            'if our OS is 32 bit, then no need to evaluate 64 bit drivers
            If Environment.Is64BitOperatingSystem = False Then
                ret_val = ace_version + ";32"
                Return ret_val
            End If

            'check main and wow64
            Dim key_path_1 As String = "HKEY_CLASSES_ROOT\CLSID\" + ace_clsid + "\InprocServer32"
            Dim key_path_2 As String = "HKEY_CLASSES_ROOT\WOW6432Node\CLSID\" + ace_clsid + "\InprocServer32"

            For i As Integer = 1 To 2
                search_key_path = Choose(i, key_path_1, key_path_2)
                Dim val2 As Object = My.Computer.Registry.GetValue(search_key_path, "", Nothing)
                If val2 IsNot Nothing Then
                    office_path = val2.ToString
                    If office_path.ToLower.Contains("program files (x86)") Then
                        bit_wise = "32"
                    Else
                        bit_wise = "64"
                    End If
                    ret_val = ace_version + ";" + bit_wise
                    Return ret_val
                End If
            Next
        Catch ex As Exception
            Return Nothing
        End Try

    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)>
    Public Shared Function get_dts_bin_path(ByVal use_32_bit_runtime As Boolean) As String
        Dim res As String = ""

        Dim server_version As String

        Using cn As New SqlClient.SqlConnection("context connection = true")
            cn.Open()
            Using cmd As New SqlClient.SqlCommand("select concat(left(cast(SERVERPROPERTY('ProductVersion') as varchar(30)),2),'0')", cn)
                server_version = cmd.ExecuteScalar().ToString
            End Using
        End Using

        Dim dts_bin_path As String = ""
        Dim reg_srch As String = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Microsoft SQL Server\" + server_version + "\SSIS\Setup\DTSPath"

        Dim reg_val As String = Registry.GetValue(reg_srch, "", "n/a").ToString

        dts_bin_path = reg_val + "Binn\"

        'if we are on a 32 bit system, just return the path
        If Not Environment.Is64BitOperatingSystem Then
            res = dts_bin_path
        ElseIf use_32_bit_runtime Then

            'if we detect x86
            If InStr(LCase(dts_bin_path), "program files (x86)") > 0 Then
                res = dts_bin_path
            Else
                res = Replace(dts_bin_path, "Program Files", "Program Files (x86)")
            End If
        Else
            'otherwise, 64 bit, just return path
            res = dts_bin_path
        End If

        Return res
    End Function

    <SqlFunction(DataAccess:=DataAccessKind.Read)>
    Shared Function format_qry_as_text(ByVal src_qry As String) As String

        Dim res As String = ""
        Dim dt As New DataTable()

        Dim row_index As Integer = 0
        Using cn As New SqlClient.SqlConnection("context connection = true")
            'Using cn As New SqlClient.SqlConnection("server = mddt\dev16; integrated security = true")
            cn.Open()
            Using cmd As New SqlClient.SqlCommand(src_qry, cn)
                Using rd = cmd.ExecuteReader()

                    If rd.HasRows Then
                        'get the headers and create the data table
                        For i As Integer = 0 To rd.FieldCount - 1
                            dt.Columns.Add(rd.GetName(i).ToString, GetType(String))
                        Next

                        While rd.Read
                            Dim dr = dt.NewRow
                            For i As Integer = 0 To rd.FieldCount - 1
                                dr(i) = rd(i).ToString
                            Next
                            dt.Rows.Add(dr)
                            row_index += 1
                            'dont read more than 50 row to prevent a memory issue
                            If row_index >= 50 Then Exit While
                        End While
                    End If


                End Using
            End Using

            'Using sda As New SqlClient.SqlDataAdapter(src_qry, cn)
            '    sda.Fill(dt)
            'End Using
        End Using

        Dim cols As New List(Of col_meta_data)

        'populate metadata
        For i = 0 To dt.Columns.Count - 1

            Dim dataCol As DataColumn = dt.Columns(i)

            Dim col_nm As String = dataCol.ColumnName.ToString
            Dim max_width As Integer = Len(col_nm.ToString)



            'scan all rows to get max width
            For Each row As DataRow In dt.Rows
                Dim curr_width As Integer = Len(row(i).ToString)
                If curr_width > max_width Then
                    max_width = curr_width
                End If
            Next

            'add the metadata
            Dim col As New col_meta_data With {.col_nm = col_nm, .max_len = max_width, .ord_pos = i}
            cols.Add(col)

        Next

        'Now that we have the columns and widths, format an output string
        Dim padding As Integer = 2

        Dim header_string As String = ""
        Dim dashes_under_header As String = ""
        Dim data_string As String = ""

        'add headers
        For Each col In cols
            header_string += col.col_nm + Space(col.max_len - Len(col.col_nm) + padding)
            dashes_under_header += replicate("-", col.max_len) + Space(padding)
        Next

        'build the data string
        For Each row As DataRow In dt.Rows

            For i = 0 To dt.Columns.Count - 1

                Dim item_val As String = Trim(row(i).ToString)

                data_string += item_val + Space(cols(i).max_len - Len(item_val) + padding)

            Next

            data_string += vbCrLf

        Next

        dt.Dispose()

        res = header_string + vbCrLf + dashes_under_header + vbCrLf + data_string

        Return res

    End Function

    Private Shared Function replicate(text As String, count As Integer) As String

        Dim res As String = ""
        For i = 1 To count
            res += text
        Next

        Return res

    End Function

    Private Class col_meta_data
        Property col_nm As String
        Property ord_pos As Integer
        Property max_len As Integer
    End Class





End Class

<Serializable()>
<SqlUserDefinedAggregate(Format.UserDefined, MaxByteSize:=-1, IsInvariantToNulls:=True, IsInvariantToOrder:=False, IsInvariantToDuplicates:=False, IsNullIfEmpty:=True)>
Public Structure CONCAT_LIST
    Implements IBinarySerialize

    Private _agg_string As StringBuilder
    Private _delim As StringBuilder

    Public Sub Init()
        _agg_string = New StringBuilder
        _delim = New StringBuilder
    End Sub

    Public Sub Accumulate(ByVal txt As String, ByVal delim As String)

        If Not String.IsNullOrWhiteSpace(txt) Then
            _agg_string.Append(txt)
            _agg_string.Append(delim)
        End If

        'this is the only way to keep track of the delimiter length as it goes through the process
        If _delim.Length = 0 Then
            _delim.Append(delim)
        End If

    End Sub

    Public Sub Merge(ByVal group As CONCAT_LIST)
        _agg_string.Append(group._agg_string.ToString())
    End Sub

    Public Function Terminate() As SqlString

        If _agg_string.Length = 0 Then
            Return String.Empty
        Else
            'lop off delim
            _agg_string.Remove(_agg_string.Length - _delim.Length, _delim.Length)
            Return _agg_string.ToString()
        End If

    End Function

    Private Sub IBinarySerialize_Read(r As System.IO.BinaryReader) Implements IBinarySerialize.Read
        _delim = New StringBuilder(r.ReadString())
        _agg_string = New StringBuilder(r.ReadString)
    End Sub

    Private Sub IBinarySerialize_Write(w As System.IO.BinaryWriter) Implements IBinarySerialize.Write
        w.Write(_delim.ToString())
        w.Write(_agg_string.ToString())
    End Sub

End Structure