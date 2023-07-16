Imports System.Reflection
Imports System.IO
Imports System.Data.SqlClient

Class cSQLObjects_Installer

    Shared Sub install_sql_server_objects(mssql_inst As String, ByRef err_msg As String, ByVal install_type As String)
        Dim install_step As String
        Try

            Dim asm As Assembly = Assembly.GetExecutingAssembly
            'stream in the file
            Dim file_nm As String
            If install_type = "INSTALL" Then
                file_nm = "MDDT_Install.sql"
            ElseIf install_type = "UNINSTALL" Then
                file_nm = "MDDT_Remove.sql"
            End If
            Dim stream_obj_name As String = My.Application.Info.AssemblyName + "." + file_nm

            Using stream As Stream = asm.GetManifestResourceStream(stream_obj_name)

                Using sr As StreamReader = New StreamReader(stream)
                    Dim sql_exec_cmd As String = sr.ReadToEnd()
                    sql_exec_cmd = sql_exec_cmd.Replace("{EDIS_BUILD_VERSION}", cConstants.prod_version)
                    sql_exec_cmd = sql_exec_cmd.Replace("{EDIS_ACTIVATION_TYPE}", cConstants.edis_activation_type)
                    sql_exec_cmd = sql_exec_cmd.Replace("{EDIS_USER_VERSION}", cConstants.edis_user_version)
                    sql_exec_cmd = sql_exec_cmd.Replace("{EDIS_SERIAL_NO}", cConstants.edis_serial_nbr)
                    sql_exec_cmd = sql_exec_cmd.Replace("{SQL_YEAR}", cConstants.sql_version_year)
                    ' sql_exec_cmd = sql_exec_cmd.Replace("{EDIS_RELEASE_NOTES}", cConstants.release_notes)

                    'set the proxy cetificate password...random
                    Dim proxy_certificate_password = System.Guid.NewGuid.ToString.Replace("-", "_") + "_$(N@TDS)(GSNDVCCL!!!~*&ncsolusncWINTGSO/*-/''\\23092570gsngds09729031j43l"
                    sql_exec_cmd = sql_exec_cmd.Replace("{PROXY_CERTIFICATE_PASSWORD}", proxy_certificate_password)

                    'set the proxy certificate temporary path
                    Dim proxy_temp_path As String = Trim(mSQLFunctions.mssql_temp_write_path(mssql_inst, err_msg)) + "MDDataTechEDISProxyCert.cer"
                    If err_msg <> "" Then
                        Exit Sub
                    End If
                    sql_exec_cmd = sql_exec_cmd.Replace("{PROXY_CERTIFICATE_TEMP_PATH}", proxy_temp_path)

                    'create a temp path for the clr assembly
                    Dim clr_temp_path As String = Trim(mSQLFunctions.mssql_temp_write_path(mssql_inst, err_msg)) + "MDDataTechCLR.dll"
                    If err_msg <> "" Then
                        Exit Sub
                    End If
                    sql_exec_cmd = sql_exec_cmd.Replace("{MDDT_CLR_ASM_PATH}", clr_temp_path)

                    'Break script at the "GO" points
                    Dim gos() As String = {"GO" & vbCr & vbLf, "GO ", "GO" & vbTab}

                    Dim batches() As String = sql_exec_cmd.Split(gos, StringSplitOptions.RemoveEmptyEntries)



                    'Dim SQLtxn As SqlTransaction = cn.BeginTransaction()

                    For Each batch In batches
                            Try

                                If InStr(batch, "--{INSTALL_STEP}") > 0 Then

                                    Dim install_step_start_text As Integer = InStr(batch, "--{INSTALL_STEP}") + 16
                                    Dim install_step_end_text As Integer = InStr(batch, "{/INSTALL_STEP}")

                                    install_step = Mid(batch, install_step_start_text, install_step_end_text - install_step_start_text)
                                End If

                                batch = batch.Replace("-- Copyright Notice", cConstants.copyright_notice)

                            Using cn As New SqlConnection("Server = " + mssql_inst + "; Integrated Security = True; initial catalog = SSISDB;")
                                cn.Open()

                                'since we are changing databases sometimes
                                If InStr(batch, "--{CHANGE DATABASE:MSDB}") > 0 Then
                                    cn.ChangeDatabase("MSDB")
                                ElseIf InStr(batch, "--{CHANGE DATABASE:MASTER}") > 0 Then
                                    cn.ChangeDatabase("MASTER")
                                End If

                                'clear the temp path if it exists
                                If InStr(batch, "--{ACTION:CLEAR_PROXY_CER_TEMP_PATH}") > 0 Then
                                    If IO.File.Exists(proxy_temp_path) Then
                                        IO.File.Delete(proxy_temp_path)
                                    End If
                                End If

                                'CLR DLL: remove if exists
                                If InStr(batch, "--{ACTION:ASM_KEY_DELETE_FILE_IF_EXISTS}") > 0 Then
                                    If IO.File.Exists(clr_temp_path) Then
                                        IO.File.Delete(clr_temp_path)
                                    End If
                                End If

                                'CLR DLL: write file to temp path
                                If InStr(batch, "--{ACTION:ASM_KEY_WRITE_FILE}") > 0 Then
                                    Dim clrDll = My.Resources.Resource.ResourceManager.GetObject("MDDataTechCLR")
                                    File.WriteAllBytes(clr_temp_path, clrDll)
                                End If

                                Using cmd As New SqlCommand(batch, cn)
                                    'cmd.CommandTimeout = 0
                                    cmd.ExecuteNonQuery()
                                End Using

                            End Using

                        Catch ex As SqlException
                                err_msg = "SQL Objects Install Error: " + vbCrLf
                                err_msg += "Step: " + install_step + vbCrLf
                                err_msg += "Description: " + ex.Message.ToString

                                'SQLtxn.Rollback()

                                Exit Sub
                                'Throw New Exception(ex.Message, ex)
                            End Try
                        Next

                        'SQLtxn.Commit()

                    End Using



            End Using
        Catch ex As Exception
            err_msg = "SQL Objects Install Error: " + vbCrLf
            err_msg += "Step: " + install_step + vbCrLf
            err_msg += "Description: " + ex.Message.ToString
        End Try


    End Sub



End Class
