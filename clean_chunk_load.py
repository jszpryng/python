##load a csv file, remove commas and quote-encapsulation, replace with a delimiter of your choice
import pandas as pd
from getpass import getuser

print('This code will remove commas and quotes and from your .csv file and chunk it to your specifications. ')
#vars
i=1
fn=input('Input file path: ')
o_fn=input('Output file path: ')
c_size=input('Desired chunk size: ')
c_size=int(c_size)
dlmtr=input('Desired delimiter: ')
print('Input file: '+fn+', Output file: '+o_fn+', Chunk size: '+str(c_size))

#iterate chunks
df=pd.read_csv(fn,sep=',',header=1,lineterminator='\n',chunksize=c_size)
for chunk in df:
    chunk.replace('"','',regex=True,inplace=True)
    chunk.to_csv(o_fn+str(i)+'.csv',mode='w',sep=dlmtr,index=False,header=None)
    i=i+1
    skp=c_size*i
#log
print("File "+fn+" has been chunked by "+str(c_size)+" rows with "+dlmtr+" delimiter to output file: "+o_fn)