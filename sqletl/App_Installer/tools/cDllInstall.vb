Imports System.IO
Imports Microsoft.Win32
Imports System.Text

'NOTE: The DLL installs below are set to dynamically grab the DLL based on the version...no need to update this script for future sql versions

Public Class cDllInstall
    Shared Sub process_exe(ByVal mssql_inst As String, ByRef err_msg As String, ByVal action As String)

        Select Case action
            Case "install"
                copy_exe_to_dts_bin(mssql_inst, err_msg)
            Case "uninstall"
                remove_dlls(mssql_inst, err_msg)
        End Select

    End Sub

    Private Shared Sub copy_exe_to_dts_bin(ByVal mssql_inst As String, ByRef err_msg As String)

        Try
            'get DTS bin path
            Dim dts_bin_path As String = get_dts_bin_path(mssql_inst)

            'For Each r In My.Resources.SQLETL_EDIS.ToString
            'Next



            ' Dim edis_64 = My.Resources.Resource.ResourceManager.GetObject("SQLETLEDIS")

            Dim edis_64 = My.Resources.SQLETL_EDIS
            Dim edis_32 = My.Resources.SQLETL_EDIS32
            Dim crash_monitor = My.Resources.SQLETL_EDISCrashHandler

            'MsgBox(edis_64.ToString)
            'Dim edis_32 = My.Resources.Resource.ResourceManager.GetObject("SQLETLEDIS32")
            ' Dim crash_monitor = My.Resources.Resource.ResourceManager.GetObject("SQLETL_EDISCrashHandler")

            Dim sql_version As String = mSQLFunctions.get_server_version(mssql_inst) / 10


            'BUILD THE REDIRECT CONFIG FILE

            Dim xml_config_string As New StringBuilder

            xml_config_string.AppendLine("<configuration>")
            xml_config_string.AppendLine("<startup useLegacyV2RuntimeActivationPolicy = ""true"" >")
            xml_config_string.AppendLine("<supportedRuntime version=""v4.0""/>")
            xml_config_string.AppendLine("</startup>")
            xml_config_string.AppendLine("<runtime>")
            xml_config_string.AppendLine("<!-- EDIS Redirect for backwards compatibility anchor on 2017 -->")
            xml_config_string.AppendLine("<gcServer enabled = ""true"" />")
            xml_config_string.AppendLine("<disableCommitThreadStack enabled=""true""/>")
            xml_config_string.AppendLine("<generatePublisherEvidence enabled = ""false"" />")

            xml_config_string.AppendLine("<assemblyBinding xmlns=""urn:schemas-microsoft-com:asm.v1"">")
            xml_config_string.AppendLine("<dependentAssembly>")
            xml_config_string.AppendLine("<assemblyIdentity name=""Microsoft.SqlServer.ManagedDTS"" publicKeyToken=""89845Dcd8080cc91"" culture=""neutral""/>")
            xml_config_string.AppendLine("<bindingRedirect oldVersion=""11.0.0.0 - 14.0.0.0"" newVersion=""" + sql_version.ToString() + ".0.0.0""/>")
            xml_config_string.AppendLine("</dependentAssembly>")
            xml_config_string.AppendLine("</assemblyBinding>")

            xml_config_string.AppendLine("<assemblyBinding xmlns=""urn:schemas-microsoft-com:asm.v1"">")
            xml_config_string.AppendLine("<dependentAssembly>")
            xml_config_string.AppendLine("<assemblyIdentity name=""Microsoft.SqlServer.DTSRuntimeWrap"" publicKeyToken=""89845dcd8080cc91"" culture=""neutral""/>")
            xml_config_string.AppendLine("<bindingRedirect oldVersion=""11.0.0.0 - 14.0.0.0"" newVersion=""" + sql_version.ToString() + ".0.0.0""/>")
            xml_config_string.AppendLine("</dependentAssembly>")
            xml_config_string.AppendLine("</assemblyBinding>")

            xml_config_string.AppendLine("<assemblyBinding xmlns=""urn:schemas-microsoft-com:asm.v1"">")
            xml_config_string.AppendLine("<dependentAssembly>")
            xml_config_string.AppendLine("<assemblyIdentity name=""Microsoft.SqlServer.DTSPipelineWrap"" publicKeyToken=""89845dcd8080cc91"" culture=""neutral""/>")
            xml_config_string.AppendLine("<bindingRedirect oldVersion=""11.0.0.0 - 14.0.0.0"" newVersion=""" + sql_version.ToString() + ".0.0.0""/>")
            xml_config_string.AppendLine("</dependentAssembly>")
            xml_config_string.AppendLine("</assemblyBinding>")

            xml_config_string.AppendLine("<assemblyBinding xmlns=""urn:schemas-microsoft-com:asm.v1"">")
            xml_config_string.AppendLine("<dependentAssembly>")
            xml_config_string.AppendLine("<assemblyIdentity name=""Microsoft.SqlServer.PipelineHost"" publicKeyToken=""89845dcd8080cc91"" culture=""neutral""/>")
            xml_config_string.AppendLine("<bindingRedirect oldVersion=""11.0.0.0 - 14.0.0.0"" newVersion=""" + sql_version.ToString() + ".0.0.0""/>")
            xml_config_string.AppendLine("</dependentAssembly>")
            xml_config_string.AppendLine("</assemblyBinding>")
            xml_config_string.AppendLine("</runtime>")
            xml_config_string.AppendLine("</configuration>")


            'Dim xml_config_file = My.Resources.Resource.ResourceManager.GetObject("SQLETL_EDIS" + sql_version + "_exe")

            'MsgBox(sql_version)

            'is this a 64 bit system
            If Environment.Is64BitOperatingSystem Then
                Dim x64_path As String
                Dim x86_path As String

                'if we detect x86
                If InStr(LCase(dts_bin_path), "program files (x86)") > 0 Then
                    x86_path = dts_bin_path
                    x64_path = Replace(dts_bin_path, "Program Files (x86)", "Program Files")

                Else
                    x64_path = dts_bin_path
                    x86_path = Replace(dts_bin_path, "Program Files", "Program Files (x86)")
                End If

                If Directory.Exists(x64_path) Then
                    System.IO.File.WriteAllBytes(x64_path & "SQLETL_EDIS.exe", edis_64)
                    System.IO.File.WriteAllBytes(x64_path & "SQLETL_EDISCrashHandler.exe", crash_monitor)
                    File.WriteAllText(x64_path & "SQLETL_EDIS.exe.config", xml_config_string.ToString())
                End If

                If Directory.Exists(x86_path) Then
                    System.IO.File.WriteAllBytes(x86_path & "SQLETL_EDIS.exe", edis_32)
                    System.IO.File.WriteAllBytes(x86_path & "SQLETL_EDISCrashHandler.exe", crash_monitor)
                    File.WriteAllText(x86_path & "SQLETL_EDIS.exe.config", xml_config_string.ToString())
                End If
            Else
                'for 32 bit systems
                System.IO.File.WriteAllBytes(dts_bin_path & "SQLETL_EDIS.exe", edis_32)
                System.IO.File.WriteAllBytes(dts_bin_path & "SQLETL_EDISCrashHandler.exe", crash_monitor)
                File.WriteAllText(dts_bin_path & "SQLETL_EDIS.exe.config", xml_config_string.ToString())
            End If


        Catch ex As Exception
            err_msg = ex.Message + ex.StackTrace.ToString()
            'MsgBox(err_msg)
        End Try

    End Sub

    Private Shared Sub remove_dlls(ByVal mssql_inst As String, ByRef err_msg As String)

        Try
            Dim default_dll_path As String = get_dts_bin_path(mssql_inst)

            Dim x64_path As String
            Dim x86_path As String

            'if we detect x86
            If InStr(LCase(default_dll_path), "program files (x86)") > 0 Then

                x86_path = default_dll_path
                x64_path = Replace(default_dll_path, "Program Files (x86)", "Program Files")

            Else
                x64_path = default_dll_path
                x86_path = Replace(default_dll_path, "Program Files", "Program Files (x86)")
            End If

            Dim sql_version As String = mSQLFunctions.get_server_version(mssql_inst)


            If File.Exists(x86_path & "MDDataTech" + sql_version + ".dll") Then
                File.Delete(x86_path & "MDDataTech" + sql_version + ".dll")
            End If

            If File.Exists(x86_path & "EDISLauncher.exe") Then
                File.Delete(x86_path & "EDISLauncher.exe")
            End If

            If File.Exists(x64_path & "MDDataTech" + sql_version + ".dll") Then
                File.Delete(x64_path & "MDDataTech" + sql_version + ".dll")
            End If

            If File.Exists(x64_path & "EDISLauncher.exe") Then
                File.Delete(x64_path & "EDISLauncher.exe")
            End If

            If File.Exists(x86_path & "SQLETL_EDIS.exe") Then
                File.Delete(x86_path & "SQLETL_EDIS.exe")
            End If

            If File.Exists(x64_path & "SQLETL_EDIS.exe") Then
                File.Delete(x64_path & "SQLETL_EDIS.exe")
            End If

            '----------------------------------------------------------------------------------------------------------
            'Remove reg keys
            Dim baseKey As RegistryKey
            If Environment.Is64BitOperatingSystem Then
                baseKey = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry64)
            Else
                baseKey = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry32)
            End If

            baseKey.DeleteSubKey("Software\SQLETL.COM\" + Replace(LCase(mssql_inst), "\", "_"))
            baseKey.Close()


        Catch ex As Exception
            err_msg = ex.Message
            'MsgBox(err_msg)
        End Try


    End Sub

    Private Shared Function get_dts_bin_path(ByVal mssql_inst As String) As String

        Dim vsn As String = get_server_version(mssql_inst)
        Dim reg_srch As String = "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Microsoft SQL Server\" + vsn + "\SSIS\Setup\DTSPath"

        Dim reg_val As String = Registry.GetValue(reg_srch, "", "n/a").ToString

        reg_val += "Binn\"

        Return reg_val

    End Function

End Class
