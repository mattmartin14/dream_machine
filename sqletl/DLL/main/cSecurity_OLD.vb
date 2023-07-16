
Imports System.Security.Cryptography
Imports System.Text

Class cSecurity_OLD

    Private Const expiration_msg As String =
            "** EDIS TRIAL EXPIRATION NOTICE ** " + vbCrLf +
            "-> Your EDIS trial has expired. If you would like to continue using this program, please visit www.sqletl.com and purchase a full license. "

    Shared Sub check_sec_settings(ByVal mssql_inst As String, ByVal install_id As String, ByRef err_msg As String)

        Dim is_trial As Boolean = False

        Dim sql_inst_nm As String = Replace(mssql_inst, "\", "_")

        Dim trial_hash As String, full_hash As String

        trial_hash = Left(get_hash("trial", sql_inst_nm), 32)
        full_hash = Left(get_hash("full", sql_inst_nm), 32)

        install_id = Trim(Left(install_id, 32))

        If install_id = trial_hash Then

            'Check for trial expiration
            Dim sql As String = ""
            sql += "select max(success_cnt) as success_cnt "
            sql += "from ( "
            sql += "select isnull((select count(*) from SSISDB.catalog.executions where folder_name = 'SQLETL.COM' "
            sql += "   and project_name = 'EDIS' "
            sql += "   and status = 7),0) as success_cnt "
            sql += "   union all "
            sql += "select isnull((SELECT count(distinct operation_Id) "
            sql += "FROM [SSISDB].[catalog].[operations] "
            sql += "  where status = 7 "
            sql += "and object_name = 'EDIS'),0) as success_cnt "
            sql += " union all "
            sql += "select isnull(( "
            sql += " select count(distinct operation_id) "
            sql += " from SSISDB.catalog.event_messages o "
            sql += " where package_name = 'Launcher.dtsx' "
            sql += " and event_name = 'OnPostExecute' "
            sql += "and not exists(select 1 from SSISDB.catalog.event_messages where event_name = 'OnError' and operation_id = o.operation_id)),0) as success_cnt "
            sql += ") as sub "

            Dim success_cnt As Integer
            Using cn As New SqlClient.SqlConnection("server = " + mssql_inst + "; integrated security = true")
                cn.Open()
                Using cmd As New SqlClient.SqlCommand(sql, cn)
                    cmd.CommandTimeout = 0
                    success_cnt = CInt(cmd.ExecuteScalar())
                End Using
            End Using

            If success_cnt >= 50 Then
                err_msg = expiration_msg
            End If

        ElseIf install_id <> full_hash Then

            err_msg = "The registered edition of EDIS on this server does not match the license."
            err_msg += vbCrLf
            err_msg += "Please contact support@sqletl.com for help. "
        End If

    End Sub

    Private Shared Function get_hash(ByVal version As String, ByVal sql_srvr_name As String) As String

        Dim res As String = ""
        Dim hash_string As String = ""
        Select Case version
            Case "full"
                hash_string = sql_srvr_name + "_XL$#NCAG$%*&_FULL"
            Case "trial"
                hash_string = sql_srvr_name + "_%@KSNBS)NCGE_TRIAL"
        End Select

        Using MDHash As MD5 = MD5.Create()

            Dim data As Byte() = MDHash.ComputeHash(Encoding.UTF8.GetBytes(hash_string))
            Dim sBuilder As New StringBuilder()

            Dim i As Integer
            For i = 0 To data.Length - 1
                sBuilder.Append(data(i).ToString("x2"))
            Next i

            res = sBuilder.ToString()

        End Using

        Return res

    End Function

End Class
