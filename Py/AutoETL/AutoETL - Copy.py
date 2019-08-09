from builtins import print

import pyodbc
import re
from io import StringIO





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

conn = pyodbc.connect('DRIVER={SQL Server}; SERVER=reportssql-intg; DATABASE=int0Reports; Trusted_Connection=yes')

cursor = conn.cursor()

cursor.execute(script)
conn.commit()


'''cursor.execute('select * from RETL.State')

for row in cursor:
    print(row)

'''






            
       
       
      

