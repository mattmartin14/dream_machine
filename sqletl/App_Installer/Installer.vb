
Imports System.Security.Principal
Imports System.Management
Imports System.Diagnostics
Imports System.Reflection
Public Class Installer

    Shared try_cnt As Integer = 0

    Private Sub Installer_Load(ByVal sender As Object, ByVal e As System.EventArgs) Handles MyBase.Load

        Try
            If Not is_running_as_admin() Then

                try_cnt += 1

                If try_cnt >= 2 Then
                    MsgBox("Installing EDIS requires adminstrative privileges.", vbCritical)
                    Windows.Forms.Application.Exit()
                    Exit Sub
                End If

                Dim ps As ProcessStartInfo = New ProcessStartInfo(Assembly.GetEntryAssembly().CodeBase)

                ps.UseShellExecute = True
                ps.Verb = "runas"

                Process.Start(ps)

                Windows.Forms.Application.Exit()

            End If
        Catch ex As Exception
            Dim err_msg As String = ex.Message.ToString.ToLower
            If err_msg Like "*operation was canceled by*" Then
                MsgBox("Installing EDIS requires adminstrative privileges.", vbCritical)
                Windows.Forms.Application.Exit()
                Exit Sub
            End If
        End Try


    End Sub

    Private Function is_running_as_admin() As Boolean
        Dim winID As WindowsIdentity = WindowsIdentity.GetCurrent()
        Dim p As WindowsPrincipal = New WindowsPrincipal(winID)
        Return p.IsInRole(WindowsBuiltInRole.Administrator)
    End Function



    '============================================================================================================================
    '                   INSTALLATION BUTTONS AND TEXT
    '============================================================================================================================

    Private Sub btn_welcome_install_Click(sender As Object, e As EventArgs) Handles btn_welcome_install.Click
        p_install_info.Visible = True

        btn_trial_version.Text = "EDIS Standard"
        btn_full_verision.Text = "EDIS Pro"

        'edit 2018-02-026: disabled: company closed
        btn_full_verision.Checked = True
        txt_license_key.Text = "PRO SITE LICENSE"
        txt_license_key.Enabled = False
        btn_trial_version.Enabled = False
        btn_full_verision.Enabled = False

        Dim eula_new As String = "
   **END USER LICENSE AGREEMENT (EULA) for EDIS (THE SOFTWARE PRODUCT)**

PLEASE THOROUGHLY READ THIS EULA BEFORE DETERMINING WHETHER OR NOT YOU WANT TO INSTALL THIS SOFTWARE.

This End User License Agreement is a legal agreement between you (THE END-USER) (either an individual or a single entity) and (THE COMPANY) The original company that created this software is closed and no longer in business.

By installing, copying, or otherwise using THE SOFTWARE PRODUCT, you agree to be bounded by the terms of this EULA. If you do not agree to the terms of this EULA, do not install or use THE SOFTWARE PRODUCT.

1. SOFTWARE PRODUCT LICENSE AND TRANSFERRABILITY

The license provided is a site license. It is built to support SQL Server Standard and Enterprise 2012-2017. There is no obiligation or guarantee for it to be updated to support future releases of SQL Server.
2. TERMINATION

Without prejudice to any other rights, THE COMPANY may terminate this EULA if you fail to comply with the terms and conditions of this EULA. In such event, you (THE END-USER) must destroy all copies of THE SOFTWARE PRODUCT and all of its component parts.

3. USE OF WORK

THE END-USER will not attempt to decompile, reverse-engineer, or use the code within THE SOFTWARE PRODUCT as your own work or attempt to sell, distribute, or otherwise disclose the contents within THE SOFTWARE PRODUCT without prior written consent of THE COMPANY.

LIMITED WARRANTY

4. NO WARRANTIES.

THE COMPANY expressly disclaims any warranty for THE SOFTWARE PRODUCT. THE SOFTWARE PRODUCT and any related documentation is provided ""As Is"" without warranty of any kind, either express or implied, including, without limitation, the implied warranties or merchantability, fitness for a particular purpose, or noninfringement. The entire risk arising out of use or performance of THE SOFTWARE PRODUCT remains with THE END-USER.

5. NO LIABILITY FOR DAMAGES.

In no event shall THE COMPANY be liable for any special, consequential, incidental or indirect damages whatsoever (including, without limitation, damages for loss of business profits, business interruption, loss of business information, or any other pecuniary loss) arising out of the use of or inability to use THE SOFTWARE PRODUCT.

OTHER

6. THIRD PARTY SOFTWARE NOTICE

THE SOFTWARE PRODUCT uses portions of NPOI for Excel 2003 (xls) tasks. NPOI is covered under the APACHE LICENSE and a copy of the license is obtainable HERE: http://www.apache.org/licenses/.

"
        'eula_new = "This product is built to support Home Depot SQL Servers only. Any installation outside THD is strictly prohibited."

        txt_eula_info.Text = eula_new
        'txt_eula_info.Text = "This software is as is. The original company that created this software is no longer in business. User assumes full responsibility."

        txt_install_outcome_body.Text = "thank you for installing edis"


    End Sub

    Private Sub btn_install_info_prev_Click(sender As Object, e As EventArgs) Handles btn_install_info_prev.Click
        p_install_info.Visible = False
    End Sub

    Private Sub btn_full_verision_CheckedChanged(sender As Object, e As EventArgs) Handles btn_full_verision.CheckedChanged
        lbl_license_key.Visible = True
        lbl_offline_act.Visible = True
        txt_license_key.Visible = True
    End Sub

    Private Sub btn_trial_version_CheckedChanged(sender As Object, e As EventArgs) Handles btn_trial_version.CheckedChanged
        lbl_license_key.Visible = False
        lbl_offline_act.Visible = False
        txt_license_key.Visible = False
    End Sub


    Private Sub btn_p_install_info_next_Click(sender As Object, e As EventArgs) Handles btn_p_install_info_next.Click

        Dim err_msg As String

        'SQL Instance Validation
        If Trim(txt_sql_instance.Text) = "" Then
            MsgBox("Please enter the SQL Server Instance", vbCritical, "SQL Server Instance Required")
            Exit Sub
        End If

        'Trial/Full Version Serial Validation
        If Me.btn_full_verision.Checked = True Then
            If Trim(txt_license_key.Text) = "" Then
                MsgBox("Please enter a license key if installing the full version.", vbCritical, "License Key Required")
                Exit Sub
            End If
        End If

        If btn_full_verision.Checked = False And btn_trial_version.Checked = False Then
            MsgBox("Please select either the 'EDIS Standard' or 'EDIS Pro' radio button for installation", vbCritical, "Version Specification Required")
            Exit Sub
        End If

        'run security checks
        cSecChecks.run_sec_checks(Trim(txt_sql_instance.Text), err_msg, "install")
        If err_msg <> "" Then
            MsgBox(err_msg, vbCritical, "EDIS Security Check Error")
            Exit Sub
        End If


        p_eula.Visible = True
        btn_eula_next.Enabled = False
        btn_eula_accept.Checked = False
    End Sub

    Private Sub btn_eula_accept_CheckedChanged(sender As Object, e As EventArgs) Handles btn_eula_accept.CheckedChanged
        If btn_eula_accept.Checked = True Then
            btn_eula_next.Enabled = True
        Else
            btn_eula_next.Enabled = False
        End If
    End Sub

    Private Sub btn_eula_next_Click(sender As Object, e As EventArgs) Handles btn_eula_next.Click

        '-------------------------------------------------------------------------------------------------
        'Set up final message on installation screen

        Dim real_sql_inst As String = mSQLFunctions.get_mssql_inst_full_nm(Trim(txt_sql_instance.Text))
        Dim sql_version As String = mSQLFunctions.get_server_version(Trim(txt_sql_instance.Text))

        Dim server_year As String = get_sql_vsn_year(Trim(txt_sql_instance.Text))

        Dim msg As String = ""
        msg += "Confirmation: " + vbCrLf + vbCrLf
        msg += "You are about to install EDIS with the following setup: " + vbCrLf
        msg += "-> Instance: " + real_sql_inst + vbCrLf
        msg += "-> Instance Version: MSSQL " + server_year + vbCrLf
        msg += "-> Installation Type: "
        If btn_trial_version.Checked = True Then
            msg += "Standard" + vbCrLf
            ' msg += "Note: For trial installation, EDIS will run for 50 successful executions, after which the trial will expire and the software will not run."
        Else
            msg += "Pro" + vbCrLf
            msg += "-> License Number: " + Trim(txt_license_key.Text)
        End If
        msg += vbCrLf + vbCrLf + vbCrLf
        msg += "Please click 'Install' after confirming the setup"

        lbl_confirmation_setup.Text = msg


        p_confirmation_page.Visible = True
    End Sub

    Private Sub btn_decline_eula_CheckedChanged(sender As Object, e As EventArgs) Handles btn_decline_eula.CheckedChanged
        If btn_decline_eula.Checked = True Then
            btn_eula_next.Enabled = False
        End If
    End Sub


    Private Sub btn_eula_prev_Click(sender As Object, e As EventArgs) Handles btn_eula_prev.Click
        p_eula.Visible = False
        p_install_info.Visible = True
    End Sub

    Private Sub btn_install_EDIS_PREV_Click(sender As Object, e As EventArgs) Handles btn_install_EDIS_PREV.Click
        p_confirmation_page.Visible = False
        p_eula.Visible = True
    End Sub

    Private Sub btn_install_EDIS_NEXT_Click(sender As Object, e As EventArgs) Handles btn_install_EDIS_NEXT.Click

        '-------------------------------------------------------------------------------
        'Steps for installing EDIS
        '   [1] Run Security Checks
        '   [2] verify license and install SQL objects
        '   [3] register DLL's on GAC
        '   [4] copy DLL's to 110 and 120 folder
        '--------------------------------------------------------------------------------

        Dim err_msg As String = ""
        Dim Result_header As String = "Success!"
        Dim result_body As String = "Thank you for installing EDIS. For more information about the product including tutorials, please visit www.sqletl.com"

        'edit 2/26/2018: remove website ref
        result_body = "thank you for installing EDIS"

        Dim mssql_inst As String = Trim(txt_sql_instance.Text)

        '------------------------------------------------------------
        '[1] Security Checks
        cSecChecks.run_sec_checks(mssql_inst, err_msg, "install")
        If err_msg <> "" Then

            Result_header = "EDIS Security Check Error"
            result_body = err_msg
            GoTo display_final_msg
        End If

        'update global vars
        cConstants.initialize_global_vars(mssql_inst)

        '------------------------------------------------------------



        '[2] Verify License and install SQL Objects
        Dim license_nbr As String = Trim(txt_license_key.Text)

        If btn_trial_version.Checked = True Then
            license_nbr = "trial"
            cConstants.edis_user_version = "STANDARD"
            cConstants.edis_serial_nbr = "NA"
            cConstants.edis_activation_type = "ONLINE"
        Else
            cConstants.edis_user_version = "PRO"
            cConstants.edis_serial_nbr = license_nbr
            If InStr(license_nbr, "-") = 0 Then
                cConstants.edis_activation_type = "OFFLINE"
            Else
                cConstants.edis_activation_type = "ONLINE"
            End If
        End If

        Dim result_header_color As String = "black"

        'edit 2/26/18: skip license check. no more company :(
        GoTo skip_license_check

        cRunInstallScripts.active_license(cConstants.full_sql_instance_name, license_nbr, err_msg, "install")
        ' - Is an offline activation needed?
        If Microsoft.VisualBasic.Left(LCase(err_msg), 8) = "offline:" Then

            Dim reference_id As String = Microsoft.VisualBasic.Replace(err_msg, "offline:", "")

            'set header and body
            Dim offline_msg As String = ""
            offline_msg += "Please Note: During the Installation, there was a communication error when attempting to contact the license server. "
            offline_msg += "In order to continue, please email support@sqletl.com with the information below. Offline activation requests are handled within 24 hours. "
            offline_msg += "We apologize for the inconvenience."
            offline_msg += vbCrLf + vbCrLf
            offline_msg += "To: support@sqletl.com"
            offline_msg += vbCrLf
            offline_msg += "Email Subject: EDIS Offline Activation Request"
            offline_msg += vbCrLf
            offline_msg += "Email Body:"
            offline_msg += vbCrLf
            offline_msg += "1) License Number: " + license_nbr + vbCrLf
            offline_msg += "2) Unique Reference ID: " + reference_id

            Result_header = "Offline Activation Required"
            result_header_color = "red"
            result_body = offline_msg
            GoTo display_final_msg

        End If

        If err_msg <> "" Then
            Result_header = "Error Installing EDIS License"
            result_body = err_msg
            result_header_color = "red"
            GoTo display_final_msg
        End If

skip_license_check:

        '-------------------------------------------------------------------------------
        'Install SQL Objects

        cSQLObjects_Installer.install_sql_server_objects(mssql_inst, err_msg, "INSTALL")
        If Not String.IsNullOrEmpty(err_msg) Then
            Result_header = "Error Installing SQL Objects"
            result_body = err_msg
            result_header_color = "red"
            GoTo display_final_msg
        End If

        '------------------------------------------------------------
        '[3] Register DLL's on GAC

        'read powershell text
        'Dim gac_ps As String = cTextTools.read_resource_txt_file("EDIS_App_Installer.EDIS_GAC.txt", err_msg)

        'If err_msg <> "" Then
        '    Result_header = "Error Getting GAC Installer"
        '    result_body = "There was an error attempting to setup GAC registration for EDIS"
        '    GoTo display_final_msg
        'End If

        ''set log path
        'Dim ps_log_path As String = cTextTools.get_temp_file_path()
        'gac_ps = Replace(gac_ps, "{LOG_FILE}", """" + ps_log_path + """")

        ''extract dll
        ''Dim sql_version As String = mSQLFunctions.get_server_version(mssql_inst)

        'Dim gac_temp_path As String = cTextTools.get_temp_path + "MDDataTech" + cConstants.dts_folder_nbr + ".dll"

        'Dim row_id_dll_path As String = ""
        'Dim xml_destination_dll_path As String = ""

        'If cConstants.dts_folder_nbr = "110" Then
        '    System.IO.File.WriteAllBytes(gac_temp_path, My.Resources.MDDataTech110)
        'ElseIf cConstants.dts_folder_nbr = "120" Then
        '    System.IO.File.WriteAllBytes(gac_temp_path, My.Resources.MDDataTech120)
        'ElseIf cConstants.dts_folder_nbr = "130" Then
        '    System.IO.File.WriteAllBytes(gac_temp_path, My.Resources.MDDataTech130)
        'End If

        'gac_ps = Replace(gac_ps, "{DLL_PATH}", """" + gac_temp_path + """")

        'cPowerShell.RunScript(gac_ps)

        ''read the log file to check for errors
        'Dim ps_res As String = cTextTools.read_file(ps_log_path, err_msg)
        'If Mid(ps_res, 1, 7) <> "success" Then

        '    Result_header = "Error Registering EDIS to GAC"
        '    result_header_color = "red"
        '    result_body = err_msg
        '    GoTo display_final_msg
        'End If


        'cFileTools.delete_file(ps_log_path, err_msg)
        'cFileTools.delete_file(gac_temp_path, err_msg)

        '------------------------------------------------------------
        '[4] Save DLL's to DTS Binn folders

        cDllInstall.process_exe(mssql_inst, err_msg, "install")

        If err_msg <> "" Then
            Result_header = "Error Saving DLL's to DTS Binn Folder"
            result_body = err_msg
            result_header_color = "red"
            GoTo display_final_msg
        End If


        'NOTIFICATION FOR legacy
        Dim legacy_tbl_msg As String = mSQLFunctions.check_for_legacy_svc_tbl(mssql_inst, err_msg)
        If err_msg <> "" Then
            Result_header = "Error Checking for legacy svc id table"
            result_body = err_msg
            result_header_color = "red"
            GoTo display_final_msg
        End If

        If legacy_tbl_msg <> "" Then
            MsgBox(legacy_tbl_msg, vbInformation, "Legacy table conversion notice")
        End If



display_final_msg:


        'set header for the outcome panel
        lbl_install_outcome_header.Text = Result_header

        Select Case result_header_color.ToLower
            Case "black"
                lbl_install_outcome_header.ForeColor = Color.Black
            Case "red"
                lbl_install_outcome_header.ForeColor = Color.Red

                'Raise Alert
                MsgBox("There was an error installing EDIS. Please click ok to view error information.", vbCritical, "Error Installing EDIS")

        End Select



        txt_install_outcome_body.Text = result_body

        p_confirmation_page.Visible = False
        p_final_page_after_install.Visible = True


    End Sub

    Private Sub btn_exit_installer_Click(sender As Object, e As EventArgs) Handles btn_exit_installer.Click

        p_final_page_after_install.Visible = False
        p_eula.Visible = False
        p_install_info.Visible = False
        p_welcome.Visible = True

        'Me.Close()
    End Sub




    '============================================================================================================================
    '                   Uninstall UI
    '============================================================================================================================


    Private Sub btn_welcome_remove_Click(sender As Object, e As EventArgs) Handles btn_welcome_remove.Click
        p_uninstall_setup.Visible = True
        p_welcome.Visible = False
    End Sub

    Private Sub btn_EDIS_uninstall_PREV_Click(sender As Object, e As EventArgs) Handles btn_EDIS_uninstall_PREV.Click
        p_uninstall_setup.Visible = False
        p_welcome.Visible = True
    End Sub

    Private Sub btn_EDIS_RUN_UNINSTALL_Click(sender As Object, e As EventArgs) Handles btn_EDIS_RUN_UNINSTALL.Click

        Try
            Dim err_msg As String = ""

            Dim resp = MsgBox("Please Confirm: Are you sure you want to uninstall EDIS?", vbYesNo, "Removal Confirmation")
            If resp = vbNo Then
                MsgBox("Uninstall cancelled!", vbInformation, "Uninstall Aborted")
                Exit Sub
            End If

            'run uninstall
            Dim mssql_inst As String = Trim(txt_uninstall_sql_instance.Text)
            If mssql_inst = "" Then
                MsgBox("Please provide a SQL Server Instance for the uninstall", vbCritical, "SQL Instance Required")
            End If

            '================================================================================================================
            'Run Uninstall Process
            '   Steps:
            '       [1] Remove SQL Objects
            '       [2] Remove DLL's

            'Security Check
            cSecChecks.run_sec_checks(mssql_inst, err_msg, "uninstall")
            If err_msg <> "" Then
                MsgBox(err_msg, vbCritical, "EDIS Security Check Error")
                Exit Sub
            End If

            '-------------------------------------------------------
            '   [1] Remove sql objects

            cSQLObjects_Installer.install_sql_server_objects(mssql_inst, err_msg, "UNINSTALL")

            '-------------------------------------------------------------------------------------------------
            '   [2] remove DLLs

            cDllInstall.process_exe(mssql_inst, err_msg, "uninstall")


            If err_msg <> "" Then
                err_msg = "There was an error while attempting the uninstall: " + err_msg
                MsgBox(err_msg, vbCritical, "Error During Uninstall")
            Else
                MsgBox("Uninstall Successful. Thank you for trying EDIS. We hope to see you come back!", vbInformation, "Result")
            End If


        Catch ex As Exception
            Dim ex_err_msg As String = "There was an unknown error while attempting to uninstall EDIS."
            ex_err_msg += vbCrLf
            ex_err_msg += "Error Message: " + ex.Message.ToString

            MsgBox(ex_err_msg, vbCritical, "Error During Uninstall")
        End Try


    End Sub

    Private Sub Label4_Click(sender As Object, e As EventArgs) Handles Label4.Click

    End Sub
End Class