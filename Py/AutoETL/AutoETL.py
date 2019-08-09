from builtins import print
import pyodbc
import re
from io import StringIO
import os, sys, glob

def gensp(read_data):
       s=re.search(r'(?<=--\*\*pyetl1).+(?=--\*\*pyetl2)', read_data, re.DOTALL|re.IGNORECASE|re.MULTILINE)
       s1=re.search(r'.+(?=--\*\*pyetl1)', read_data, re.DOTALL|re.IGNORECASE|re.MULTILINE)
       s2=re.search(r'(?<=--\*\*pyetl2).+', read_data, re.DOTALL|re.IGNORECASE|re.MULTILINE)
       ct = s.group(0) + '\ninto RETL.' + table
       repl = re.compile('select', re.IGNORECASE)
       ct = repl.sub('select top 0',ct) 
       script = s1.group()+ct+s2.group()
       return script

def genusp(read_data):
    cursor.execute(qview)
    rows = cursor.fetchall()
    createvw = ''
    for row in rows:
        createvw += row.columna + '\n' + ','

    p = re.compile(r',\Z')
    createvw = p.sub('',createvw)
    
    insert = 'insert into [RETL].[' + table + '](\n' + createvw + '\n)'
    read_data = read_data.replace('--**pyetl1',insert)
    read_data = read_data.replace('--**pyetl2','')
    read_data = read_data.replace("'","''")

    script = """
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

-- Name of database the stored prodedure will be in
DECLARE @DBName NVARCHAR(MAX) = 'Reports'

-- Declare the name of the stored procedure
DECLARE @StoredProcedureName nVARCHAR(MAX) = 'usp_""" + table + '\'' + """

-- Get the environment the query is running against and assign appropriate database prefix
DECLARE @DBPrefix VARCHAR(4) = LEFT(DB_NAME(),4);

Declare @DBPrefName NVARCHAR(MAX) = @DBPrefix + @DBName

-- Create query to execute SP creation
DECLARE @UseAndExec NVARCHAR(MAX) = 'USE ' + @DBPrefName + ' EXEC(@vSQLCommand)'

DECLARE @TestAndCreate NVARCHAR(MAX) = 
'USE ' + @DBPrefName + 
' IF OBJECT_ID(N''[RETL].['+ @StoredProcedureName +']'',N''P'') IS NULL
EXEC (''CREATE PROCEDURE [RETL].['+ @StoredProcedureName +'] AS SELECT 1'')'

-- Create dynamic query for creation of stored procedure
DECLARE @vSQLCommand NVARCHAR(MAX) =
'ALTER PROCEDURE [RETL].['+ @StoredProcedureName +'] 
AS
BEGIN

TRUNCATE TABLE int0Reports.[RETL].[""" + table + ']\n' + read_data + 'END\n\'\n\n' + """
EXEC(@TestAndCreate)

SET @vSQLCommand = REPLACE(@vSQLCommand,'int0',@DBPrefix)

EXEC(@vSQLCommand)
"""

    return script


def gentable(query):
    cursor.execute(query)
    rows = cursor.fetchall()
    createtab = ''
    for row in rows:
        createtab += row.columndef +',' +'\n'

    createtab = """
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
DECLARE @ScriptName varchar(128)
DECLARE @RowNumber bigint = 0;
SET @ScriptName = """ + '\'RETL_' + table + '_Create.sql\'' + """

IF (SELECT [dbo].[ufns_IsVersionDeployed] (@ScriptName)) = 'False'
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;
    CREATE TABLE [RETL].[""" + table + """] (
""" + createtab + """
)


    EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'RETL', @level0type=N'SCHEMA',@level0name=N'RETL', @level1type=N'TABLE',@level1name=N""" + '\'' + table + '\'' + """
       
    DECLARE @RC int
        DECLARE @ErrorMessage varchar(2048)		
        EXEC @RC = [dbo].[usp_VersionChangeImplemented] @ScriptName, @ScriptName, @ErrorMessage
        IF @RC <> 0
        BEGIN
            RAISERROR(@ErrorMessage,11,1);
        END

        COMMIT;
        PRINT 'Script ' + @ScriptName + ' has been deployed.';
    END TRY
    BEGIN CATCH
        SELECT 
            ERROR_NUMBER() AS ErrorNumber,
            ERROR_SEVERITY() AS ErrorSeverity,
            ERROR_STATE() as ErrorState,
            ERROR_PROCEDURE() as ErrorProcedure,
            ERROR_LINE() as ErrorLine,
            ERROR_MESSAGE() as ErrorMessage;
    ROLLBACK TRANSACTION;
    DECLARE @ErrMessage varchar(256)
    SET @ErrMessage = 'ERROR: Failed to apply ' + @ScriptName
    RAISERROR(@ErrMessage,11,1);
    END CATCH;
END
ELSE
BEGIN
    PRINT 'Script ' + @ScriptName + ' has already been deployed.';
END
GO
"""
    createtab = createtab.replace(',\n\n)','\n)')
    return createtab

def genview(qview):
    cursor.execute(qview)
    rows = cursor.fetchall()
    createvw = ''
    for row in rows:
        createvw += row.columna + '\n' + ','

    p = re.compile(r',\Z')
    createvw = p.sub('',createvw)
    createvw = """
IF OBJECT_ID(N'[RETL].[vw_""" + table + """',N'V') IS NULL
EXEC ('CREATE VIEW [RETL].[vw_""" + table + """] AS SELECT 1 as dummy')
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER VIEW [RETL].[vw_""" + table + """]
AS

SELECT""" + '\n' + createvw + '\n' + """
    
FROM [RETL].[""" + table + """]
GO
GRANT SELECT ON [RETL].[vw_""" + table + """] TO [ugReporting] AS [dbo]
GO
"""

    return createvw
    

conn = pyodbc.connect('DRIVER={SQL Server}; SERVER=reportssql-intg; DATABASE=int0Reports; Trusted_Connection=yes')
cursor = conn.cursor()

#curpath = os.getcwd()
curpath = os.path.abspath(os.path.dirname(sys.argv[0]))
os.chdir(curpath)
for f in glob.glob('pyetl_*.sql'):
    if f is not None:       
       table = f[6:f.index('.sql')]
       fpath = curpath + '\\' + f
       otable = curpath + '\\RETL_' + table + '_Create.sql'
       oview = curpath + '\\RETL.vw_' + table + '.sql'
       osp = curpath + '\\RETL.usp_' + table + '.sql'
       query=""" 
select '  ['+column_name+'] ' 
        + data_type 
		+ case data_type
               when 'sql_variant' then ''
               when 'text' then ''
               when 'ntext' then ''
               when 'xml' then ''
               when 'decimal' then '(' + cast(numeric_precision as varchar) + ', ' + cast(numeric_scale as varchar) + ')'
               else coalesce('('+case when character_maximum_length = -1 then 'MAX' else cast(character_maximum_length as varchar) end +')','') 
		   end columndef
from information_schema.columns 
where table_schema='RETL'
      and  table_name=""" + '\'' + table + '\''

       qview = """
select '['+column_name+']' as columna
from information_schema.columns 
where table_schema='RETL'
      and  table_name=""" + '\'' + table + '\''

      

       with open(fpath) as f:
           read_data = f.read()

       cursor.execute(gensp(read_data))
       conn.commit()

       with open(otable,"w") as f2:
           f2.write(gentable(query))

       with open(oview,"w") as f3:
           f3.write(genview(qview))

       with open(osp,"w") as f4:
           f4.write(genusp(read_data))
       







            
       
       
      

