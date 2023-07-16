<Global.Microsoft.VisualBasic.CompilerServices.DesignerGenerated()> _
Partial Class Form1
    Inherits System.Windows.Forms.Form

    'Form overrides dispose to clean up the component list.
    <System.Diagnostics.DebuggerNonUserCode()> _
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
    <System.Diagnostics.DebuggerStepThrough()> _
    Private Sub InitializeComponent()
        Me.btn_gen_offline_code = New System.Windows.Forms.Button()
        Me.txt_hardware_id = New System.Windows.Forms.TextBox()
        Me.Label1 = New System.Windows.Forms.Label()
        Me.txt_offline_code = New System.Windows.Forms.TextBox()
        Me.SuspendLayout()
        '
        'btn_gen_offline_code
        '
        Me.btn_gen_offline_code.Location = New System.Drawing.Point(110, 101)
        Me.btn_gen_offline_code.Name = "btn_gen_offline_code"
        Me.btn_gen_offline_code.Size = New System.Drawing.Size(124, 35)
        Me.btn_gen_offline_code.TabIndex = 0
        Me.btn_gen_offline_code.Text = "Generate Offline Code"
        Me.btn_gen_offline_code.UseVisualStyleBackColor = True
        '
        'txt_hardware_id
        '
        Me.txt_hardware_id.Location = New System.Drawing.Point(86, 58)
        Me.txt_hardware_id.Name = "txt_hardware_id"
        Me.txt_hardware_id.Size = New System.Drawing.Size(173, 20)
        Me.txt_hardware_id.TabIndex = 1
        Me.txt_hardware_id.TextAlign = System.Windows.Forms.HorizontalAlignment.Center
        '
        'Label1
        '
        Me.Label1.AutoSize = True
        Me.Label1.Location = New System.Drawing.Point(125, 33)
        Me.Label1.Name = "Label1"
        Me.Label1.Size = New System.Drawing.Size(95, 13)
        Me.Label1.TabIndex = 2
        Me.Label1.Text = "Enter Hardware ID"
        '
        'txt_offline_code
        '
        Me.txt_offline_code.Location = New System.Drawing.Point(44, 173)
        Me.txt_offline_code.Multiline = True
        Me.txt_offline_code.Name = "txt_offline_code"
        Me.txt_offline_code.Size = New System.Drawing.Size(279, 68)
        Me.txt_offline_code.TabIndex = 3
        Me.txt_offline_code.TextAlign = System.Windows.Forms.HorizontalAlignment.Center
        '
        'Form1
        '
        Me.AutoScaleDimensions = New System.Drawing.SizeF(6.0!, 13.0!)
        Me.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font
        Me.ClientSize = New System.Drawing.Size(355, 273)
        Me.Controls.Add(Me.txt_offline_code)
        Me.Controls.Add(Me.Label1)
        Me.Controls.Add(Me.txt_hardware_id)
        Me.Controls.Add(Me.btn_gen_offline_code)
        Me.Name = "Form1"
        Me.Text = "Offline Activator Tool"
        Me.ResumeLayout(False)
        Me.PerformLayout()

    End Sub
    Friend WithEvents btn_gen_offline_code As System.Windows.Forms.Button
    Friend WithEvents txt_hardware_id As System.Windows.Forms.TextBox
    Friend WithEvents Label1 As System.Windows.Forms.Label
    Friend WithEvents txt_offline_code As System.Windows.Forms.TextBox

End Class
