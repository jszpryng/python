### Union files, upload to s3
### Jordan Springer

import os
import shutil
import arrow
now = arrow.now().format('YYYY-MM-DD')
import boto3
from botocore.client import Config
import pandas as pd
from getpass import getuser

usr = getuser()
usr = usr.replace('.','_')

## union files into one file
rs1 = pd.read_csv("foo.csv")
rs1.replace({ r'\r\n': ' '}, regex=True, inplace=True)
rs2 = pd.read_csv("bar.csv")
rs2.replace({ r'\r\n': ' '}, regex=True, inplace=True)
rs = pd.concat([rs1,rs2],sort=True)
unique_rs = rs.drop_duplicates(keep='last')
unique_rs.to_csv("foobar.csv",index=False)
print("foobar has been merged.")

## file/bucket data for upload
bkt_nm = 'mybucket'
prfx = 'user/'+usr+'/mydir/'

fl_typ1 = 'foo'
fl_typ2= 'bar'

fl_nm1 = 'foo.csv'
dst_fl_nm1 = 'foo_'+str(now)+'.csv'

fl_nm2 = 'bar.csv'
dst_fl_nm2 = 'bar_'+str(now)+'.csv'

data = open(fl_nm1,'rb')
data = open(fl_nm2,'rb')

s3 = boto3.resource('s3')
client = boto3.client('s3')
bkt = s3.Bucket(bkt_nm)

## clear /wait folder
for obj in bkt.objects.filter(Prefix=prfx+fl_typ1+'/wait/'):
	response = client.delete_object(Bucket=bkt_nm, Key=obj.key)
for obj in bkt.objects.filter(Prefix=prfx+fl_typ2+'/wait/'):
	response = client.delete_object(Bucket=bkt_nm, Key=obj.key)
print("/wait folders have been cleared.")

## upload files to /wait, /archive folders
s3.Bucket(bkt_nm).upload_file(fl_nm1,prfx+'wait/'+dst_fl_nm1)
s3.Bucket(bkt_nm).upload_file(fl_nm2,prfx+'wait/'+dst_fl_nm2)
s3.Bucket(bkt_nm).upload_file(fl_nm1,prfx+'archive/'+dst_fl_nm1)
s3.Bucket(bkt_nm).upload_file(fl_nm2,prfx+'archive/'+dst_fl_nm2)

## print logs
print ("File "+dst_fl_nm1+" has been uploaded to /wait, /archive folders.")
print ("File "+dst_fl_nm2+" has been uploaded to /wait, /archive folders.")
