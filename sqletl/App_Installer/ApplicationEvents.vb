Imports Microsoft.VisualBasic.ApplicationServices



'Imports System.Security.Principal
'Imports System.Management
'Imports System.Diagnostics
'Imports System.Reflection
Namespace My

    ' The following events are available for MyApplication:
    ' Startup: Raised when the application starts, before the startup form is created.
    ' Shutdown: Raised after all application forms are closed.  This event is not raised if the application terminates abnormally.
    ' UnhandledException: Raised if the application encounters an unhandled exception.
    ' StartupNextInstance: Raised when launching a single-instance application and the application is already active. 
    ' NetworkAvailabilityChanged: Raised when the network connection is connected or disconnected.
    Partial Friend Class MyApplication

        'Private Sub MyApplication_Startup(sender As Object, e As StartupEventArgs) Handles Me.Startup

        '    If Not is_running_as_admin() Then

        '        Dim ps As ProcessStartInfo = New ProcessStartInfo(Assembly.GetEntryAssembly().CodeBase)

        '        ps.UseShellExecute = True
        '        ps.Verb = "runas"



        '        Process.Start(ps)
        '        ' My.Application.MainForm.Close()
        '        Windows.Forms.Application.Exit()



        '    End If
        'End Sub
        'Private Function is_running_as_admin() As Boolean
        '    Dim winID As WindowsIdentity = WindowsIdentity.GetCurrent()
        '    Dim p As WindowsPrincipal = New WindowsPrincipal(winID)
        '    Return p.IsInRole(WindowsBuiltInRole.Administrator)
        'End Function

    End Class
End Namespace
