from builtins import print
import pyodbc
import re
from io import StringIO
import os, sys, glob


conn = pyodbc.connect('DRIVER={SQL Server}; SERVER=reportssql-intg; DATABASE=int0Reports; Trusted_Connection=yes')
cursor = conn.cursor()


p = re.compile('--output', re.IGNORECASE)

msg = '\\\\sunqld.com.au\\filestore\\UserData\\HomeDrives\\RChen\\Documents\\AutoETL\\RETL.usp_Employer_MonthlyClaimsTest.sql'
dir = '\\\\sunqld.com.au\\filestore\\UserData\\HomeDrives\\RChen\\Documents\\AutoETL\\'
fn = dir + 'RETL.usp_Employer_MonthlyClaimsWrite.sql'
table = 'RETL.Employer_MonthlyClaimsTest'


with open(msg) as f:
    with open(fn,"w") as f1:
       read_data = f.read()
       l = len(read_data)
       #m=p.match(read_data)
       #s=p.search(read_data)
       #s=re.search(r'--output[\w+]end',read_data)
       s=re.search(r'(?<=--\*\*pyetl1).+(?=--\*\*pyetl2)', read_data, re.DOTALL|re.IGNORECASE|re.MULTILINE)
       s1=re.search(r'.+(?=--\*\*pyetl1)', read_data, re.DOTALL|re.IGNORECASE|re.MULTILINE)
       s2=re.search(r'(?<=--\*\*pyetl2).+', read_data, re.DOTALL|re.IGNORECASE|re.MULTILINE)
       ct = s.group(0) + '\ninto ' + table
       repl = re.compile('select', re.IGNORECASE)
       ct = repl.sub('select top 0',ct) 

       #'dakdjlfda ryanpc dakfjldajla  end')    
       read_data = read_data.replace("'", "''")
       read_data = "'" + read_data + "'"
       print(read_data)
       
      
       f1.write(read_data)
       
       if s:
           #print('match', m.group())
           print('match')
       else:
           print('no match')
           
       #print(s.end(0))
       print(l)
       print(s.group(0))
       print(ct)

t="""BEGIN 

DECLARE @End AS DATE 
DECLARE @Start AS DATE 

set @End = EOMonth(DATEADD(Month,-1,GETDATE())); 

set @Start = EOMonth(DATEADD(Month,-15, @end))


delete from RETL.Employer_MonthlyClaims where MonthEndDate > @Start """

#print(ct+'\n'+t)
print(s1.group())
print(s2.group())

print(ct)

script = s1.group()+ct+s2.group()

with open(fn,"w") as f2:
    f2.write(script)

print(s1.group()+ct+s2.group())

#cursor.execute(script)
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
      and  table_name='Employer_MonthlyClaims'
"""

cursor.execute(script)
conn.commit()

cursor.execute(query)
rows = cursor.fetchall()
rn = cursor.rowcount
ss=''

for row in rows:

      ss += row.columndef +',' +'\n'

ss = """
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
DECLARE @ScriptName varchar(128)
DECLARE @RowNumber bigint = 0;
SET @ScriptName = '0510RC_RETL_Employer_MemberQuarterlyDemographics_Create.sql'

IF (SELECT [dbo].[ufns_IsVersionDeployed] (@ScriptName)) = 'False'
BEGIN
	BEGIN TRY
		BEGIN TRANSACTION;
		CREATE TABLE [RETL].[Employer_MemberQuarterlyDemographics] (
""" + ss + """
)

    CREATE NONCLUSTERED INDEX ncix_QuarterEndDate on [RETL].[Employer_MemberQuarterlyDemographics] ([QuarterEndDate]) INCLUDE (MemberKey,Plankey)

		EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'RETL for Employer_Dashboard', @level0type=N'SCHEMA',@level0name=N'RETL', @level1type=N'TABLE',@level1name=N'Employer_MemberQuarterlyDemographics'
		
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

aa = ss.replace(',\n\n)','\n)')
print(aa)
    #print(row.columndef) 


