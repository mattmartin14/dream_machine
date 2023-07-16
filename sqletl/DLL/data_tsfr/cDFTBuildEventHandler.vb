
Imports Microsoft.SqlServer.Dts.Runtime


Module err_build_msg
    Public df_task_build_err_msg As String
End Module

Class cDFTBuildEventHandler
    Implements IDTSComponentEvents

    Private Sub HandleEvent(type As String, subComponent As String, description As String)
        'Console.WriteLine("[{0}] {1}: {2}", type, subComponent, description)
    End Sub

    Public Sub FireBreakpointHit(breakpointTarget As BreakpointTarget)
    End Sub

    Public Sub FireCustomEvent(eventName As String, eventText As String, ByRef arguments As Object(), subComponent As String, ByRef fireAgain As Boolean)
    End Sub

    Public Function FireError(errorCode As Integer, subComponent As String, description As String, helpFile As String, helpContext As Integer) As Boolean
        HandleEvent("Error", subComponent, description)
        Return True
    End Function

    Public Sub FireInformation(informationCode As Integer, subComponent As String, description As String, helpFile As String, helpContext As Integer, ByRef fireAgain As Boolean)
        HandleEvent("Information", subComponent, description)
    End Sub

    Public Sub FireProgress(progressDescription As String, percentComplete As Integer, progressCountLow As Integer, progressCountHigh As Integer, subComponent As String, ByRef fireAgain As Boolean)
    End Sub

    Public Function FireQueryCancel() As Boolean
        Return True
    End Function

    Public Sub FireWarning(warningCode As Integer, subComponent As String, description As String, helpFile As String, helpContext As Integer)
        HandleEvent("Warning", subComponent, description)
    End Sub

    Public Sub FireBreakpointHit1(breakpointTarget As BreakpointTarget) Implements IDTSComponentEvents.FireBreakpointHit

    End Sub

    Public Sub FireCustomEvent1(eventName As String, eventText As String, ByRef arguments() As Object, subComponent As String, ByRef fireAgain As Boolean) Implements IDTSComponentEvents.FireCustomEvent

    End Sub

    Public Function FireError1(errorCode As Integer, subComponent As String, description As String, helpFile As String, helpContext As Integer) As Boolean Implements IDTSComponentEvents.FireError
        df_task_build_err_msg = description
    End Function

    Public Sub FireInformation1(informationCode As Integer, subComponent As String, description As String, helpFile As String, helpContext As Integer, ByRef fireAgain As Boolean) Implements IDTSComponentEvents.FireInformation

    End Sub

    Public Sub FireProgress1(progressDescription As String, percentComplete As Integer, progressCountLow As Integer, progressCountHigh As Integer, subComponent As String, ByRef fireAgain As Boolean) Implements IDTSComponentEvents.FireProgress

    End Sub

    Public Function FireQueryCancel1() As Boolean Implements IDTSComponentEvents.FireQueryCancel

    End Function

    Public Sub FireWarning1(warningCode As Integer, subComponent As String, description As String, helpFile As String, helpContext As Integer) Implements IDTSComponentEvents.FireWarning

    End Sub
End Class
