CREATE DATABASE BAD_IDEA
GO
USE BAD_IDEA
GO
CREATE OR ALTER PROCEDURE dbo.sp_do_some_bad_stuff
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @i INT = 1;

    WHILE @i <= 5
        BEGIN
            PRINT 'This is a bad idea to run MSSQL on a mac';
            SET @i += 1;
        END
END
GO