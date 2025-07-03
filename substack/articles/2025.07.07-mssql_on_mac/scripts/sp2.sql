USE BAD_IDEA
GO
CREATE OR ALTER PROCEDURE dbo.usp_test
AS
BEGIN
    SET NOCOUNT ON; 

    declare @i int = 1;

    exec sp_executesql N'SELECT @i;', N'@i int', @i = @i;

END
GO