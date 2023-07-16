<Global.Microsoft.VisualBasic.CompilerServices.DesignerGenerated()>
Partial Class Installer
    Inherits System.Windows.Forms.Form

    'Form overrides dispose to clean up the component list.
    <System.Diagnostics.DebuggerNonUserCode()>
    Protected Overrides Sub Dispose(ByVal disposing As Boolean)
        Try
            If disposing AndAlso components IsNot Nothing Then
                components.Dispose()
            End If
        Finally
            MyBase.Dispose(disposing)
        End Try
    End Sub

    'Required by the Windows Form Designer
    Private components As System.ComponentModel.IContainer

    'NOTE: The following procedure is required by the Windows Form Designer
    'It can be modified using the Windows Form Designer.  
    'Do not modify it using the code editor.
    <System.Diagnostics.DebuggerStepThrough()>
    Private Sub InitializeComponent()
        Dim resources As System.ComponentModel.ComponentResourceManager = New System.ComponentModel.ComponentResourceManager(GetType(Installer))
        Me.p_welcome = New System.Windows.Forms.Panel()
        Me.p_install_info = New System.Windows.Forms.Panel()
        Me.lbl_offline_act = New System.Windows.Forms.Label()
        Me.btn_install_info_prev = New System.Windows.Forms.Button()
        Me.btn_p_install_info_next = New System.Windows.Forms.Button()
        Me.Label5 = New System.Windows.Forms.Label()
        Me.txt_license_key = New System.Windows.Forms.TextBox()
        Me.btn_full_verision = New System.Windows.Forms.RadioButton()
        Me.btn_trial_version = New System.Windows.Forms.RadioButton()
        Me.lbl_license_key = New System.Windows.Forms.Label()
        Me.Label2 = New System.Windows.Forms.Label()
        Me.txt_sql_instance = New System.Windows.Forms.TextBox()
        Me.Label1 = New System.Windows.Forms.Label()
        Me.btn_welcome_remove = New System.Windows.Forms.Button()
        Me.btn_welcome_install = New System.Windows.Forms.Button()
        Me.PictureBox1 = New System.Windows.Forms.PictureBox()
        Me.p_eula = New System.Windows.Forms.Panel()
        Me.p_final_page_after_install = New System.Windows.Forms.Panel()
        Me.txt_install_outcome_body = New System.Windows.Forms.TextBox()
        Me.lbl_install_outcome_header = New System.Windows.Forms.Label()
        Me.btn_exit_installer = New System.Windows.Forms.Button()
        Me.p_confirmation_page = New System.Windows.Forms.Panel()
        Me.btn_install_EDIS_PREV = New System.Windows.Forms.Button()
        Me.btn_install_EDIS_NEXT = New System.Windows.Forms.Button()
        Me.lbl_confirmation_setup = New System.Windows.Forms.Label()
        Me.btn_decline_eula = New System.Windows.Forms.RadioButton()
        Me.btn_eula_accept = New System.Windows.Forms.RadioButton()
        Me.txt_eula_info = New System.Windows.Forms.TextBox()
        Me.btn_eula_prev = New System.Windows.Forms.Button()
        Me.btn_eula_next = New System.Windows.Forms.Button()
        Me.p_uninstall_setup = New System.Windows.Forms.Panel()
        Me.btn_EDIS_uninstall_PREV = New System.Windows.Forms.Button()
        Me.btn_EDIS_RUN_UNINSTALL = New System.Windows.Forms.Button()
        Me.Label4 = New System.Windows.Forms.Label()
        Me.Label3 = New System.Windows.Forms.Label()
        Me.txt_uninstall_sql_instance = New System.Windows.Forms.TextBox()
        Me.p_welcome.SuspendLayout()
        Me.p_install_info.SuspendLayout()
        CType(Me.PictureBox1, System.ComponentModel.ISupportInitialize).BeginInit()
        Me.p_eula.SuspendLayout()
        Me.p_final_page_after_install.SuspendLayout()
        Me.p_confirmation_page.SuspendLayout()
        Me.p_uninstall_setup.SuspendLayout()
        Me.SuspendLayout()
        '
        'p_welcome
        '
        Me.p_welcome.Controls.Add(Me.p_install_info)
        Me.p_welcome.Controls.Add(Me.btn_welcome_remove)
        Me.p_welcome.Controls.Add(Me.btn_welcome_install)
        Me.p_welcome.Location = New System.Drawing.Point(0, 64)
        Me.p_welcome.Name = "p_welcome"
        Me.p_welcome.Size = New System.Drawing.Size(502, 275)
        Me.p_welcome.TabIndex = 0
        '
        'p_install_info
        '
        Me.p_install_info.Controls.Add(Me.lbl_offline_act)
        Me.p_install_info.Controls.Add(Me.btn_install_info_prev)
        Me.p_install_info.Controls.Add(Me.btn_p_install_info_next)
        Me.p_install_info.Controls.Add(Me.Label5)
        Me.p_install_info.Controls.Add(Me.txt_license_key)
        Me.p_install_info.Controls.Add(Me.btn_full_verision)
        Me.p_install_info.Controls.Add(Me.btn_trial_version)
        Me.p_install_info.Controls.Add(Me.lbl_license_key)
        Me.p_install_info.Controls.Add(Me.Label2)
        Me.p_install_info.Controls.Add(Me.txt_sql_instance)
        Me.p_install_info.Controls.Add(Me.Label1)
        Me.p_install_info.Location = New System.Drawing.Point(0, 0)
        Me.p_install_info.Name = "p_install_info"
        Me.p_install_info.Size = New System.Drawing.Size(502, 275)
        Me.p_install_info.TabIndex = 2
        Me.p_install_info.Visible = False
        '
        'lbl_offline_act
        '
        Me.lbl_offline_act.AutoSize = True
        Me.lbl_offline_act.Font = New System.Drawing.Font("Microsoft Sans Serif", 8.25!, System.Drawing.FontStyle.Italic, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lbl_offline_act.Location = New System.Drawing.Point(151, 136)
        Me.lbl_offline_act.Name = "lbl_offline_act"
        Me.lbl_offline_act.Size = New System.Drawing.Size(282, 13)
        Me.lbl_offline_act.TabIndex = 12
        Me.lbl_offline_act.Text = "For Offline Activation, Enter the Offline Activation Key Only"
        Me.lbl_offline_act.Visible = False
        '
        'btn_install_info_prev
        '
        Me.btn_install_info_prev.Location = New System.Drawing.Point(28, 235)
        Me.btn_install_info_prev.Name = "btn_install_info_prev"
        Me.btn_install_info_prev.Size = New System.Drawing.Size(108, 26)
        Me.btn_install_info_prev.TabIndex = 11
        Me.btn_install_info_prev.Text = "Previous"
        Me.btn_install_info_prev.UseVisualStyleBackColor = True
        '
        'btn_p_install_info_next
        '
        Me.btn_p_install_info_next.Location = New System.Drawing.Point(368, 235)
        Me.btn_p_install_info_next.Name = "btn_p_install_info_next"
        Me.btn_p_install_info_next.Size = New System.Drawing.Size(108, 26)
        Me.btn_p_install_info_next.TabIndex = 10
        Me.btn_p_install_info_next.Text = "Next"
        Me.btn_p_install_info_next.UseVisualStyleBackColor = True
        '
        'Label5
        '
        Me.Label5.AutoSize = True
        Me.Label5.Location = New System.Drawing.Point(29, 104)
        Me.Label5.Name = "Label5"
        Me.Label5.Size = New System.Drawing.Size(135, 13)
        Me.Label5.TabIndex = 9
        Me.Label5.Text = "2) Choose Installation Type"
        '
        'txt_license_key
        '
        Me.txt_license_key.Location = New System.Drawing.Point(139, 152)
        Me.txt_license_key.Multiline = True
        Me.txt_license_key.Name = "txt_license_key"
        Me.txt_license_key.Size = New System.Drawing.Size(314, 41)
        Me.txt_license_key.TabIndex = 7
        Me.txt_license_key.Visible = False
        '
        'btn_full_verision
        '
        Me.btn_full_verision.AutoSize = True
        Me.btn_full_verision.Location = New System.Drawing.Point(224, 102)
        Me.btn_full_verision.Name = "btn_full_verision"
        Me.btn_full_verision.Size = New System.Drawing.Size(109, 17)
        Me.btn_full_verision.TabIndex = 6
        Me.btn_full_verision.TabStop = True
        Me.btn_full_verision.Text = "Install Full Version"
        Me.btn_full_verision.UseVisualStyleBackColor = True
        '
        'btn_trial_version
        '
        Me.btn_trial_version.AutoSize = True
        Me.btn_trial_version.Location = New System.Drawing.Point(350, 102)
        Me.btn_trial_version.Name = "btn_trial_version"
        Me.btn_trial_version.Size = New System.Drawing.Size(75, 17)
        Me.btn_trial_version.TabIndex = 5
        Me.btn_trial_version.TabStop = True
        Me.btn_trial_version.Text = "Install Trial"
        Me.btn_trial_version.UseVisualStyleBackColor = True
        '
        'lbl_license_key
        '
        Me.lbl_license_key.AutoSize = True
        Me.lbl_license_key.Location = New System.Drawing.Point(46, 166)
        Me.lbl_license_key.Name = "lbl_license_key"
        Me.lbl_license_key.Size = New System.Drawing.Size(77, 13)
        Me.lbl_license_key.TabIndex = 4
        Me.lbl_license_key.Text = "3) License Key"
        Me.lbl_license_key.Visible = False
        '
        'Label2
        '
        Me.Label2.AutoSize = True
        Me.Label2.Location = New System.Drawing.Point(29, 64)
        Me.Label2.Name = "Label2"
        Me.Label2.Size = New System.Drawing.Size(118, 13)
        Me.Label2.TabIndex = 2
        Me.Label2.Text = "1) SQL Server Instance"
        '
        'txt_sql_instance
        '
        Me.txt_sql_instance.Location = New System.Drawing.Point(194, 62)
        Me.txt_sql_instance.Name = "txt_sql_instance"
        Me.txt_sql_instance.Size = New System.Drawing.Size(259, 20)
        Me.txt_sql_instance.TabIndex = 1
        '
        'Label1
        '
        Me.Label1.Font = New System.Drawing.Font("Microsoft Sans Serif", 9.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.Label1.Location = New System.Drawing.Point(29, 4)
        Me.Label1.Name = "Label1"
        Me.Label1.Size = New System.Drawing.Size(441, 36)
        Me.Label1.TabIndex = 0
        Me.Label1.Text = "Welcome! " & Global.Microsoft.VisualBasic.ChrW(13) & Global.Microsoft.VisualBasic.ChrW(10) & "Please enter the information below. The installation will take less th" &
    "an 1 minute"
        Me.Label1.TextAlign = System.Drawing.ContentAlignment.TopCenter
        '
        'btn_welcome_remove
        '
        Me.btn_welcome_remove.Location = New System.Drawing.Point(189, 168)
        Me.btn_welcome_remove.Name = "btn_welcome_remove"
        Me.btn_welcome_remove.Size = New System.Drawing.Size(110, 30)
        Me.btn_welcome_remove.TabIndex = 1
        Me.btn_welcome_remove.Text = "Remove"
        Me.btn_welcome_remove.UseVisualStyleBackColor = True
        '
        'btn_welcome_install
        '
        Me.btn_welcome_install.Location = New System.Drawing.Point(176, 61)
        Me.btn_welcome_install.Name = "btn_welcome_install"
        Me.btn_welcome_install.Size = New System.Drawing.Size(138, 48)
        Me.btn_welcome_install.TabIndex = 0
        Me.btn_welcome_install.Text = "Install"
        Me.btn_welcome_install.UseVisualStyleBackColor = True
        '
        'PictureBox1
        '
        Me.PictureBox1.Image = Global.EDIS_App_Installer.My.Resources.Resource.sqletl
        Me.PictureBox1.Location = New System.Drawing.Point(0, 0)
        Me.PictureBox1.Name = "PictureBox1"
        Me.PictureBox1.Size = New System.Drawing.Size(502, 65)
        Me.PictureBox1.SizeMode = System.Windows.Forms.PictureBoxSizeMode.StretchImage
        Me.PictureBox1.TabIndex = 0
        Me.PictureBox1.TabStop = False
        '
        'p_eula
        '
        Me.p_eula.Controls.Add(Me.p_final_page_after_install)
        Me.p_eula.Controls.Add(Me.p_confirmation_page)
        Me.p_eula.Controls.Add(Me.btn_decline_eula)
        Me.p_eula.Controls.Add(Me.btn_eula_accept)
        Me.p_eula.Controls.Add(Me.txt_eula_info)
        Me.p_eula.Controls.Add(Me.btn_eula_prev)
        Me.p_eula.Controls.Add(Me.btn_eula_next)
        Me.p_eula.Location = New System.Drawing.Point(1, 64)
        Me.p_eula.Name = "p_eula"
        Me.p_eula.Size = New System.Drawing.Size(498, 272)
        Me.p_eula.TabIndex = 13
        Me.p_eula.Visible = False
        '
        'p_final_page_after_install
        '
        Me.p_final_page_after_install.Controls.Add(Me.txt_install_outcome_body)
        Me.p_final_page_after_install.Controls.Add(Me.lbl_install_outcome_header)
        Me.p_final_page_after_install.Controls.Add(Me.btn_exit_installer)
        Me.p_final_page_after_install.Location = New System.Drawing.Point(3, 0)
        Me.p_final_page_after_install.Name = "p_final_page_after_install"
        Me.p_final_page_after_install.Size = New System.Drawing.Size(495, 272)
        Me.p_final_page_after_install.TabIndex = 3
        Me.p_final_page_after_install.Visible = False
        '
        'txt_install_outcome_body
        '
        Me.txt_install_outcome_body.Location = New System.Drawing.Point(0, 43)
        Me.txt_install_outcome_body.Multiline = True
        Me.txt_install_outcome_body.Name = "txt_install_outcome_body"
        Me.txt_install_outcome_body.Size = New System.Drawing.Size(492, 168)
        Me.txt_install_outcome_body.TabIndex = 3
        '
        'lbl_install_outcome_header
        '
        Me.lbl_install_outcome_header.Font = New System.Drawing.Font("Microsoft Sans Serif", 11.25!, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lbl_install_outcome_header.Location = New System.Drawing.Point(132, 16)
        Me.lbl_install_outcome_header.Name = "lbl_install_outcome_header"
        Me.lbl_install_outcome_header.Size = New System.Drawing.Size(238, 20)
        Me.lbl_install_outcome_header.TabIndex = 2
        Me.lbl_install_outcome_header.Text = "Offline Activation Required"
        Me.lbl_install_outcome_header.TextAlign = System.Drawing.ContentAlignment.TopCenter
        '
        'btn_exit_installer
        '
        Me.btn_exit_installer.Location = New System.Drawing.Point(190, 222)
        Me.btn_exit_installer.Name = "btn_exit_installer"
        Me.btn_exit_installer.Size = New System.Drawing.Size(131, 39)
        Me.btn_exit_installer.TabIndex = 0
        Me.btn_exit_installer.Text = "Exit Installer"
        Me.btn_exit_installer.UseVisualStyleBackColor = True
        '
        'p_confirmation_page
        '
        Me.p_confirmation_page.Controls.Add(Me.btn_install_EDIS_PREV)
        Me.p_confirmation_page.Controls.Add(Me.btn_install_EDIS_NEXT)
        Me.p_confirmation_page.Controls.Add(Me.lbl_confirmation_setup)
        Me.p_confirmation_page.Location = New System.Drawing.Point(0, 1)
        Me.p_confirmation_page.Name = "p_confirmation_page"
        Me.p_confirmation_page.Size = New System.Drawing.Size(498, 274)
        Me.p_confirmation_page.TabIndex = 5
        Me.p_confirmation_page.Visible = False
        '
        'btn_install_EDIS_PREV
        '
        Me.btn_install_EDIS_PREV.Location = New System.Drawing.Point(28, 235)
        Me.btn_install_EDIS_PREV.Name = "btn_install_EDIS_PREV"
        Me.btn_install_EDIS_PREV.Size = New System.Drawing.Size(108, 26)
        Me.btn_install_EDIS_PREV.TabIndex = 2
        Me.btn_install_EDIS_PREV.Text = "Previous"
        Me.btn_install_EDIS_PREV.UseVisualStyleBackColor = True
        '
        'btn_install_EDIS_NEXT
        '
        Me.btn_install_EDIS_NEXT.Font = New System.Drawing.Font("Microsoft Sans Serif", 8.25!, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.btn_install_EDIS_NEXT.Location = New System.Drawing.Point(368, 235)
        Me.btn_install_EDIS_NEXT.Name = "btn_install_EDIS_NEXT"
        Me.btn_install_EDIS_NEXT.Size = New System.Drawing.Size(108, 26)
        Me.btn_install_EDIS_NEXT.TabIndex = 1
        Me.btn_install_EDIS_NEXT.Text = "Install"
        Me.btn_install_EDIS_NEXT.UseVisualStyleBackColor = True
        '
        'lbl_confirmation_setup
        '
        Me.lbl_confirmation_setup.Font = New System.Drawing.Font("Microsoft Sans Serif", 9.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.lbl_confirmation_setup.Location = New System.Drawing.Point(25, 17)
        Me.lbl_confirmation_setup.Name = "lbl_confirmation_setup"
        Me.lbl_confirmation_setup.Size = New System.Drawing.Size(444, 183)
        Me.lbl_confirmation_setup.TabIndex = 0
        Me.lbl_confirmation_setup.Text = resources.GetString("lbl_confirmation_setup.Text")
        '
        'btn_decline_eula
        '
        Me.btn_decline_eula.AutoSize = True
        Me.btn_decline_eula.Location = New System.Drawing.Point(175, 204)
        Me.btn_decline_eula.Name = "btn_decline_eula"
        Me.btn_decline_eula.Size = New System.Drawing.Size(61, 17)
        Me.btn_decline_eula.TabIndex = 4
        Me.btn_decline_eula.TabStop = True
        Me.btn_decline_eula.Text = "Decline"
        Me.btn_decline_eula.UseVisualStyleBackColor = True
        '
        'btn_eula_accept
        '
        Me.btn_eula_accept.AutoSize = True
        Me.btn_eula_accept.Location = New System.Drawing.Point(273, 204)
        Me.btn_eula_accept.Name = "btn_eula_accept"
        Me.btn_eula_accept.Size = New System.Drawing.Size(59, 17)
        Me.btn_eula_accept.TabIndex = 3
        Me.btn_eula_accept.TabStop = True
        Me.btn_eula_accept.Text = "Accept"
        Me.btn_eula_accept.UseVisualStyleBackColor = True
        '
        'txt_eula_info
        '
        Me.txt_eula_info.Location = New System.Drawing.Point(3, 7)
        Me.txt_eula_info.Multiline = True
        Me.txt_eula_info.Name = "txt_eula_info"
        Me.txt_eula_info.ScrollBars = System.Windows.Forms.ScrollBars.Vertical
        Me.txt_eula_info.Size = New System.Drawing.Size(495, 190)
        Me.txt_eula_info.TabIndex = 2
        Me.txt_eula_info.Text = resources.GetString("txt_eula_info.Text")
        '
        'btn_eula_prev
        '
        Me.btn_eula_prev.Location = New System.Drawing.Point(28, 235)
        Me.btn_eula_prev.Name = "btn_eula_prev"
        Me.btn_eula_prev.Size = New System.Drawing.Size(108, 26)
        Me.btn_eula_prev.TabIndex = 1
        Me.btn_eula_prev.Text = "Previous"
        Me.btn_eula_prev.UseVisualStyleBackColor = True
        '
        'btn_eula_next
        '
        Me.btn_eula_next.Enabled = False
        Me.btn_eula_next.Location = New System.Drawing.Point(368, 235)
        Me.btn_eula_next.Name = "btn_eula_next"
        Me.btn_eula_next.Size = New System.Drawing.Size(108, 26)
        Me.btn_eula_next.TabIndex = 0
        Me.btn_eula_next.Text = "Next"
        Me.btn_eula_next.UseVisualStyleBackColor = True
        '
        'p_uninstall_setup
        '
        Me.p_uninstall_setup.Controls.Add(Me.btn_EDIS_uninstall_PREV)
        Me.p_uninstall_setup.Controls.Add(Me.btn_EDIS_RUN_UNINSTALL)
        Me.p_uninstall_setup.Controls.Add(Me.Label4)
        Me.p_uninstall_setup.Controls.Add(Me.Label3)
        Me.p_uninstall_setup.Controls.Add(Me.txt_uninstall_sql_instance)
        Me.p_uninstall_setup.Location = New System.Drawing.Point(0, 64)
        Me.p_uninstall_setup.Name = "p_uninstall_setup"
        Me.p_uninstall_setup.Size = New System.Drawing.Size(502, 275)
        Me.p_uninstall_setup.TabIndex = 4
        Me.p_uninstall_setup.Visible = False
        '
        'btn_EDIS_uninstall_PREV
        '
        Me.btn_EDIS_uninstall_PREV.Font = New System.Drawing.Font("Microsoft Sans Serif", 8.25!, System.Drawing.FontStyle.Bold, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.btn_EDIS_uninstall_PREV.Location = New System.Drawing.Point(28, 235)
        Me.btn_EDIS_uninstall_PREV.Name = "btn_EDIS_uninstall_PREV"
        Me.btn_EDIS_uninstall_PREV.Size = New System.Drawing.Size(108, 26)
        Me.btn_EDIS_uninstall_PREV.TabIndex = 4
        Me.btn_EDIS_uninstall_PREV.Text = "Exit"
        Me.btn_EDIS_uninstall_PREV.UseVisualStyleBackColor = True
        '
        'btn_EDIS_RUN_UNINSTALL
        '
        Me.btn_EDIS_RUN_UNINSTALL.Location = New System.Drawing.Point(368, 235)
        Me.btn_EDIS_RUN_UNINSTALL.Name = "btn_EDIS_RUN_UNINSTALL"
        Me.btn_EDIS_RUN_UNINSTALL.Size = New System.Drawing.Size(108, 26)
        Me.btn_EDIS_RUN_UNINSTALL.TabIndex = 3
        Me.btn_EDIS_RUN_UNINSTALL.Text = "Remove EDIS"
        Me.btn_EDIS_RUN_UNINSTALL.UseVisualStyleBackColor = True
        '
        'Label4
        '
        Me.Label4.Font = New System.Drawing.Font("Microsoft Sans Serif", 9.0!, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, CType(0, Byte))
        Me.Label4.Location = New System.Drawing.Point(58, 68)
        Me.Label4.Name = "Label4"
        Me.Label4.Size = New System.Drawing.Size(395, 152)
        Me.Label4.TabIndex = 2
        Me.Label4.Text = resources.GetString("Label4.Text")
        '
        'Label3
        '
        Me.Label3.AutoSize = True
        Me.Label3.Location = New System.Drawing.Point(55, 35)
        Me.Label3.Name = "Label3"
        Me.Label3.Size = New System.Drawing.Size(134, 13)
        Me.Label3.TabIndex = 1
        Me.Label3.Text = "Enter SQL Server Instance"
        '
        'txt_uninstall_sql_instance
        '
        Me.txt_uninstall_sql_instance.Location = New System.Drawing.Point(208, 32)
        Me.txt_uninstall_sql_instance.Name = "txt_uninstall_sql_instance"
        Me.txt_uninstall_sql_instance.Size = New System.Drawing.Size(217, 20)
        Me.txt_uninstall_sql_instance.TabIndex = 0
        '
        'Installer
        '
        Me.AutoScaleDimensions = New System.Drawing.SizeF(6.0!, 13.0!)
        Me.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font
        Me.ClientSize = New System.Drawing.Size(497, 336)
        Me.Controls.Add(Me.p_uninstall_setup)
        Me.Controls.Add(Me.p_eula)
        Me.Controls.Add(Me.PictureBox1)
        Me.Controls.Add(Me.p_welcome)
        Me.Name = "Installer"
        Me.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen
        Me.Text = "Installation Center"
        Me.p_welcome.ResumeLayout(False)
        Me.p_install_info.ResumeLayout(False)
        Me.p_install_info.PerformLayout()
        CType(Me.PictureBox1, System.ComponentModel.ISupportInitialize).EndInit()
        Me.p_eula.ResumeLayout(False)
        Me.p_eula.PerformLayout()
        Me.p_final_page_after_install.ResumeLayout(False)
        Me.p_final_page_after_install.PerformLayout()
        Me.p_confirmation_page.ResumeLayout(False)
        Me.p_uninstall_setup.ResumeLayout(False)
        Me.p_uninstall_setup.PerformLayout()
        Me.ResumeLayout(False)

    End Sub

    Friend WithEvents p_welcome As Panel
    Friend WithEvents PictureBox1 As PictureBox
    Friend WithEvents btn_welcome_remove As Button
    Friend WithEvents btn_welcome_install As Button
    Friend WithEvents p_install_info As Panel
    Friend WithEvents Label1 As Label
    Friend WithEvents Label5 As Label
    Friend WithEvents txt_license_key As TextBox
    Friend WithEvents btn_full_verision As RadioButton
    Friend WithEvents btn_trial_version As RadioButton
    Friend WithEvents lbl_license_key As Label
    Friend WithEvents Label2 As Label
    Friend WithEvents txt_sql_instance As TextBox
    Friend WithEvents btn_install_info_prev As Button
    Friend WithEvents btn_p_install_info_next As Button
    Friend WithEvents lbl_offline_act As Label
    Friend WithEvents p_eula As Panel
    Friend WithEvents btn_decline_eula As RadioButton
    Friend WithEvents btn_eula_accept As RadioButton
    Friend WithEvents txt_eula_info As TextBox
    Friend WithEvents btn_eula_prev As Button
    Friend WithEvents btn_eula_next As Button
    Friend WithEvents p_confirmation_page As Panel
    Friend WithEvents btn_install_EDIS_PREV As Button
    Friend WithEvents btn_install_EDIS_NEXT As Button
    Friend WithEvents lbl_confirmation_setup As Label
    Friend WithEvents p_final_page_after_install As Panel
    Friend WithEvents txt_install_outcome_body As TextBox
    Friend WithEvents lbl_install_outcome_header As Label
    Friend WithEvents btn_exit_installer As Button
    Friend WithEvents p_uninstall_setup As Panel
    Friend WithEvents btn_EDIS_uninstall_PREV As Button
    Friend WithEvents btn_EDIS_RUN_UNINSTALL As Button
    Friend WithEvents Label4 As Label
    Friend WithEvents Label3 As Label
    Friend WithEvents txt_uninstall_sql_instance As TextBox
End Class
