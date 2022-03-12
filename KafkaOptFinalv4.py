
import pyspark
from pyspark.sql import SparkSession

from pyspark.sql.functions import*
from pyspark.sql.types import*
import time

import os

import numpy as np
import re
import pandas as pd 
import traceback
import sys
from time import sleep
import traceback
import smtplib
from collections import defaultdict

import ibm_db
import ibm_db_dbi
import ibm_db_sa
from datetime import datetime, date, time


# In[ ]:


spark = SparkSession.builder.getOrCreate()


import logging
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
spark.sparkContext.setLogLevel("ERROR")
# In[ ]:

######## Grab the Arguments #########

print("**Arg*****")
argslist = sys.argv
print("********")


########## Function to Send Email #########

def sendmail(sub, body):
    try:
       SERVER = "us.relay.ibm.com"
       #SERVER = "smtp.gmail.com"
       #SERVER = smtplib.SMTP('smtp.gmail.com', 587)
       #SERVER.ehlo()
       FROM = "romdbload@radial.com"
       TO = ["mlakshm@us.ibm.com"] # must be a list
       SUBJECT = sub
       TEXT = body
       message = 'From:'+FROM+       '\nTo: '+", ".join(TO)+       '\nSubject: '+SUBJECT+'\n'+TEXT
       print(message)
       server = smtplib.SMTP(SERVER)
       server.sendmail(FROM, TO, message)
       server.quit()
       return 0
    except Exception as e:
       exp_tb=traceback.format_exc()
       print(exp_tb)



# In[ ]:


tbllist=[]
jsonpathlists=[]
colslist=[]
json_exploded_path_lists=[]
col_len_lists=[]

with open('/myapp/json_column_map_updated_newest1.txt') as f:
    contents = f.read()
    contents=contents.strip()
    maplist=contents.split("TABLE_NAME:")
    #print(maplist)
    #maplist = list(filter([], maplist))
    for lmap in maplist:
        
        lmaplist=lmap.split("\n")
        #lmaplist = list(filter(None, lmaplist))
        tbl=lmaplist.pop(0)
        tbllist.append(tbl)
        collist=[]
        jsonpathlist=[]
        json_exploded_path_list=[]
        col_len_list=[]
        
        for col_json in lmaplist:
            coljsonarr=col_json.split(",")
            if (len(coljsonarr)==4):
               collist.append(coljsonarr[0])
               jsonpathlist.append(coljsonarr[1])
               json_exploded_path_list.append(coljsonarr[2])
               col_len_list.append(coljsonarr[3])
               
                
        colslist.append(collist)
        jsonpathlists.append(jsonpathlist)
        json_exploded_path_lists.append(json_exploded_path_list)
        col_len_lists.append(col_len_list)
            
            
f.close()


# In[ ]:


tbllist = [x for x in tbllist if x != 'KAFKARADIAL.']
tbllist = [x for x in tbllist if x != '']
colslist = [x for x in colslist if x != []]
jsonpathlists = [x for x in jsonpathlists if x != []]
json_exploded_path_lists = [x for x in json_exploded_path_lists if x != []]
col_len_lists = [x for x in col_len_lists if x != []]


# In[ ]:


print(tbllist)
print(colslist)
print(jsonpathlists)
print(json_exploded_path_lists)
print(col_len_lists)


# In[ ]:


jsondf = spark.read.json("/myapp/order_created_test3.json", multiLine=True)
order_created_schema=jsondf.schema


# In[ ]:


jsondf.printSchema()


# In[ ]:


#df = spark   .readStream   .format("kafka")   .option("kafka.bootstrap.servers", "minimal-prod-kafka-bootstrap-cp4i.itzroks-270006dwv1-tdbuym-6ccd7f378ae819553d37d5f2ee142bd6-0000.us-south.containers.appdomain.cloud:443")   .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username='cred2' password='yRenuVTcidCV';")   .option("kafka.security.protocol", "SASL_SSL")   .option("kafka.sasl.mechanism", "SCRAM-SHA-512")   .option("kafka.ssl.truststore.location","/myapp/es-cert.jks")   .option("kafka.ssl.truststore.password", "JCi4nt0DkN9B")   .option("kafka.ssl.protocol", "TLSv1.2")   .option("kafka.ssl.enabled.protocols", "TLSv1.2")   .option("kafka.ssl.endpoint.identification.algorithm", "HTTPS")   .option("subscribe", "gtest2")   .load()   .select(col("value").cast("string"),from_json(col("value").cast("string"), order_created_schema))



# In[ ]:

    
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "minimal-prod-kafka-bootstrap-cp4i.itzroks-270006dwv1-tdbuym-6ccd7f378ae819553d37d5f2ee142bd6-0000.us-south.containers.appdomain.cloud:443") \
  .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username='cred2' password='yRenuVTcidCV';") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
  .option("kafka.ssl.truststore.location","/myapp/es-cert.jks") \
  .option("kafka.ssl.truststore.password", "JCi4nt0DkN9B") \
  .option("kafka.ssl.protocol", "TLSv1.2") \
  .option("kafka.ssl.enabled.protocols", "TLSv1.2") \
  .option("kafka.ssl.endpoint.identification.algorithm", "HTTPS") \
  .option("failOnDataLoss", "false") \
  .option("assign", """{"gtest2":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]}""") \
  .load() \
  .select(col("partition").cast("string"),col("offset").cast("string"),col("value").cast("string"),from_json(col("value").cast("string"), order_created_schema))

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "minimal-prod-kafka-bootstrap-cp4i.itzroks-270006dwv1-tdbuym-6ccd7f378ae819553d37d5f2ee142bd6-0000.us-south.containers.appdomain.cloud:443") \
  .option("kafka.sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username='cred2' password='yRenuVTcidCV';") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
  .option("kafka.ssl.truststore.location","/myapp/es-cert.jks") \
  .option("kafka.ssl.truststore.password", "JCi4nt0DkN9B") \
  .option("kafka.ssl.protocol", "TLSv1.2") \
  .option("kafka.ssl.enabled.protocols", "TLSv1.2") \
  .option("kafka.ssl.endpoint.identification.algorithm", "HTTPS") \
  .option("failOnDataLoss", "false") \
  .option("assign", """{"gtest50p":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49]}""") \
  .load() \
  .select(col("partition").cast("string"),col("offset").cast("string"),col("value").cast("string"),from_json(col("value").cast("string"), order_created_schema))



df.printSchema()




# In[ ]:


jdbcUrl="jdbc:db2://67beb010-4100-48d1-a281-7b5af97791aa.bs2ipa7w0uv9tsomi9ig.databases.appdomain.cloud:31815/bludb:sslConnection=true;"
user="a5406f1c"
password="eK1EhADAU7y3W61T"


# In[ ]:


def writeToSQLWarehouse(jdf,epochId,tbl):
    
  print(tbl)
  #print(df)
  print("hgjghjgh")
  jdf.show()
  print("jhghjgjn")
  jdf.write.format("jdbc")   .mode("append")   .option("url", jdbcUrl)   .option("dbtable", tbl)   .option("user", user)   .option("password", password)   .save()


# In[ ]:


arraysdf = pd.read_csv("/myapp/arrays_order_created.txt", sep=",")


# In[ ]:


def explodeDF(df):
    
    df=df.withColumn("order", df.mvalue.order)
    df=df.withColumn("customAttributes", df.mvalue.customAttributes)
    df=df.withColumn("topicName", df.mvalue.topicName)
    df=df.drop("mvalue")
    #df=df.drop("value")
    #df.show()
    
    for i, row in arraysdf.iterrows():
    
     try:
            
        json_path_to_explode=row['JSON_PATH_TO_EXPLODE']
        json_path_exploded=row['JSON_PATH_EXPLODED']
        print(json_path_to_explode)
        print(json_path_exploded)
    
        df=df.withColumn(json_path_exploded,explode_outer(json_path_to_explode))
        #df.show()
        
     except Exception as e:
        print(e)
        
        
    return df


# In[ ]:


def connectToDB():
    #logger.info("Connecting to the Database....")
    print("Connecting to DB")
    conn=""
    try:
       array = { ibm_db.SQL_ATTR_AUTOCOMMIT : ibm_db.SQL_AUTOCOMMIT_OFF }
       db2_conn = ibm_db.connect("DATABASE=BLUDB;HOSTNAME=10.5.197.84;PORT=30480;PROTOCOL=TCPIP;UID=admin;PWD=B09Y0kZgchfo;", "", "", array)
       #db2_conn = ibm_db.connect("DATABASE=BLUDB;HOSTNAME=67beb010-4100-48d1-a281-7b5af97791aa.bs2ipa7w0uv9tsomi9ig.databases.appdomain.cloud;PORT=31815;PROTOCOL=TCPIP;UID=a5406f1c;PWD=eK1EhADAU7y3W61T;SECURITY=SSL", "", "", array)
       #db2_conn = ibm_db.connect("jdbc:db2://67beb010-4100-48d1-a281-7b5af97791aa.bs2ipa7w0uv9tsomi9ig.databases.appdomain.cloud:31815/bludb:sslConnection=true;", "a5406f1c", "eK1EhADAU7y3W61T")
       print(db2_conn)
    
       #conn = ibm_db_dbi.Connection(db2_conn)
       #conn.set_autocommit(False)
    except Exception as e:
       exp_tb=traceback.format_exc()
       print(exp_tb)
       sendmail("Error Retrieving Last Run Date",exp_tb)
       
    return db2_conn


# In[ ]:


def popTablesBlkAtomicNew(df, epochId):  #### Bulk Insert
      
   df.persist()   
   #####  Create JDBC Connection #######
   print(datetime.now())
   conn=connectToDB()
      
   try:
      
    if df.rdd.isEmpty():
       print(df.rdd.isEmpty)
    else:
      #df.show()
      df=df.withColumnRenamed("from_json(CAST(value AS STRING))", "mvalue") 
      #df=df.repartition(200)
      #df.select(col("mvalue").cast("string")).show()
      #valchk=NULL
      #dfnull=df.filter(col("mvalue").cast("string") == '{null, null, null}')
      #df=df.filter("mvalue is NOT NULL")
    
      #dfnull.show()
        
      #df=df.filter(col("mvalue").cast("string") != '{null, null, null}')
    
      #if dfnull.rdd.isEmpty():
            
      #      print("Empty")
            
      #else:
            
      #      nulldflist=dfnull.select("value").collect()
            
      #      for elm in nulldflist:
      #          flist=[]
      #          fval=elm[0]
      #          fparam=tuple([fval])
      #          fqry="INSERT INTO KAFKARADIAL.INSERT_FAILED_MSG ( msgvalue ) VALUES (?)"
      #          flist.append([fqry,fparam])
      #          insertData(flist,conn)
    
    
     
    
      if df.rdd.isEmpty():
       print(df.rdd.isEmpty)
    
      else:
    
         ###### Exploding Dataframe #######
         print("Starting to Explode Data:........")
         print(datetime.now()) 
         dfblk=explodeDF(df) 
         print("Finished Exploding Data:........")
         print(datetime.now()) 
      
          
         ##### Prepare Blk Dataframe & Insert Queries  #######
          
         #qlist,finaldflist=prepareData(dfblk,json_exploded_path_lists,colslist,epochId,tbllist)
         #qlist,finaldflist,dictlist,colslistup=prepareData(dfblk,json_exploded_path_lists,colslist,col_len_lists,epochId,tbllist)
         print("Starting to Prepare Data:........")
         print(datetime.now())  
         qlist=prepareData(dfblk,json_exploded_path_lists,colslist,col_len_lists,epochId,tbllist)
         print(datetime.now())
         print("Finishing to Prepare Data:.......")
    
         #print(qlist)
         print("*****")
         #print(len(finaldflist))   
            
         ###### Insert to Tables #######
         try:
             print("Starting to Insert:........")
             print(datetime.now())   
             insertData(qlist,conn)
             print(datetime.now())
             print("Finishing to Insert:........")
        
         except Exception as e:
                
             print(e)
             exp_tb=traceback.format_exc()
             print(exp_tb)
                
             #performSingleInserts(finaldflist,dictlist,colslistup,epochId,tbllist,conn)
            
    
   except Exception as e:
          print("here...........")
          print(e)
          exp_tb=traceback.format_exc()
          print(exp_tb)
          #popTablesAtomicUpdated(df, epochId, conn)
          
   finally:
         
          if conn==True:
            #print(conn)
            ibm_db.close(conn)
          df.unpersist()


# In[ ]:


def  performSingleInserts(dfblk,json_exploded_path_lists,colslist,epochId,tbllist,conn):
    
    print("Entering Single Inserts......")
    
    try:  
      
      if conn==False:
          conn=connectToDB()    
      
      ibm_db.rollback(conn)
    
      dictlist,colslistup=prepareDataSingle(dfblk,json_exploded_path_lists,colslist,epochId,tbllist) 
      insSingleData(dictlist,tbllist,colslistup,conn)
    
            
    except Exception as e:
    
          print("hereëee....")
          exp_tb=traceback.format_exc()
          print(exp_tb)


# In[ ]:


def  performSingleInserts(finaldflist,dictlist,colslistup,epochId,tbllist,conn):
    
    print("Entering Single Inserts......")
    print(len(finaldflist))
    
    try:  
      
      if conn==False:
          conn=connectToDB()    
      
      ibm_db.rollback(conn)
    
      #dictlist,colslistup=prepareDataSingle(finaldflist,colslist,epochId,tbllist) 
      insSingleData(dictlist,tbllist,colslistup,conn)
    
            
    except Exception as e:
    
          print("hereëee....")
          exp_tb=traceback.format_exc()
          print(exp_tb)


# In[ ]:


def insertSingleData(qlist, conn):
    print("single ins")
    #print(qlist)
    try:
    
          qry=""
          param=""
            
          for q in qlist:
             qry=q[0]
             param=q[1]
            
             #print(qry)
             #print(param)
             
             stmt = ibm_db.prepare(conn, qry)
             #print("here*"+qry)  
             #print(param)  
             #print("here***")
             #print(qry)
             #print(type(param))
             #print(param)
             ibm_db.execute(stmt, param)
                
          ibm_db.commit(conn) 
        
    except Exception as e:
            
             print("hereëee....")
             exp_tb=traceback.format_exc()
             print(exp_tb)
             ibm_db.rollback(conn)
             flist=[]
             fqry="INSERT INTO KAFKARADIAL.INSERT_FAILED_MSG ( msgvalue ) VALUES (?)"
             paramstr=','.join(param)
             fval=qry+"\n"+"("+paramstr+")"
             print(fval)
             fparam=tuple([fval])
             print(fparam)
             flist.append([fqry,fparam])
             insertData(flist,conn)
             return


# In[ ]:


def prepareDataOld(dfblk,json_exploded_path_lists,colslist,epochId,tbllist):
    
  qlist=[]  
  
    
  for json_exploded_path_list,romlistfinal1,tbl in zip(json_exploded_path_lists,colslist,tbllist):
    
        finaldf=prepareDFXA(dfblk,json_exploded_path_list,romlistfinal1,epochId,tbl)  
        column_names = finaldf.columns
        column_names_str=','.join(column_names)
        qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
        listval=[]
       
        ctr=0
        paramlist=[]
        
        for row in finaldf.collect():
            list2=[]
            
            for column in column_names:
                colval=row[column]
                if colval == False:
                    colval="0"
                elif colval == True:
                    colval="1"
                paramlist.append(colval)
                list2.append("?")
            str1 = ','.join(list2)
            str1='('+str1+')'
            #listval.append(str1)
            #listvalstr=','.join(listval)
            if ctr==0:
              qry_str=qry_str+str1
              ctr=ctr+1
            else:
              qry_str=qry_str+" UNION ALL \n VALUES "+str1
            #paramlist.append(tuple(list1))
        paramtup=tuple(paramlist)
        qlist.append([qry_str,paramtup])
        
        
  return qlist


# In[ ]:


def split_list(alist, wanted_parts):
    length = len(alist)
    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts] 
             for i in range(wanted_parts) ]


# In[ ]:


def prepareData(dfblk,json_exploded_path_lists,colslist,epochId,tbllist):
    
  qlist=[]  
  finaldflist=[]
  
    
  for json_exploded_path_list,romlistfinal1,tbl in zip(json_exploded_path_lists,colslist,tbllist):
    
    finaldf=prepareDFXA(dfblk,json_exploded_path_list,romlistfinal1,epochId,tbl)  
    finaldflist.append(finaldf)
    column_names = finaldf.columns
    column_names_str=','.join(column_names)
    qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES " 
    listval=[]
       
    
    
    
    fullrows = finaldf.collect()
    fullrows = list(set(fullrows))
    
    lenarr = len(fullrows)
    lenmid = lenarr//2
    
    rowsfirst = fullrows[:lenmid]
    rowssecond = fullrows[lenmid:]
    
    splitrowslist=[]
    splitrowslist.append(rowsfirst)
    splitrowslist.append(rowssecond) 
        
    splitrowslist=split_list(fullrows, 10)
    splitrowslist = [x for x in splitrowslist if x != []]
    
    #print(splitrowslist)
    
    
    for splitrows in splitrowslist:
        
        paramlist=[]
        qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES"
        ctr=0
        
        for row in splitrows:
            
          if row:
            
            
            list2=[]
            
            for column in column_names:
                colval=row[column]
                if colval == False:
                    colval="0"
                elif colval == True:
                    colval="1"
                paramlist.append(colval)
                list2.append("?")
                
            str1 = ','.join(list2)
            str1='('+str1+')'
            #listval.append(str1)
            #listvalstr=','.join(listval)
            if ctr==0:
              
              qry_str=qry_str+str1
              ctr=ctr+1
            else:
              qry_str=qry_str+" UNION ALL \n VALUES "+str1
            #paramlist.append(tuple(list1))
        paramtup=tuple(paramlist)
        qlist.append([qry_str,paramtup])
    
  
  print(len(finaldflist))
        
  return qlist,finaldflist


# In[ ]:


def prepareData(dfblk,json_exploded_path_lists,colslist,epochId,tbllist):
    
    
  qlist=[]  
  finaldflist=[]
    
  dictlist=[]
  colslistup=[]
  
    
  for json_exploded_path_list,romlistfinal1,tbl in zip(json_exploded_path_lists,colslist,tbllist):
    
    finaldf=prepareDFXA(dfblk,json_exploded_path_list,romlistfinal1,epochId,tbl)  
    finaldflist.append(finaldf)
    column_names = finaldf.columns
    column_names_str=','.join(column_names)
    colslistup.append(column_names)
    
    
    qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
    listval=[]
       
    
    fullrows = finaldf.collect()
    fullrows = list(set(fullrows))
    
    lenarr = len(fullrows)
    lenmid = lenarr//2
    
    rowsfirst = fullrows[:lenmid]
    rowssecond = fullrows[lenmid:]
    
    splitrowslist=[]
    splitrowslist.append(rowsfirst)
    splitrowslist.append(rowssecond) 
        
    splitrowslist=split_list(fullrows, 20)
    splitrowslist = [x for x in splitrowslist if x != []]
    
    #print(splitrowslist)
    d = defaultdict(list)
    
    
    for splitrows in splitrowslist:
        
        paramlist=[]
        qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
        ctr=0
        
        
        for row in splitrows:
            
          if row:
            
            ordid=row["ORDER_ID"]
            list2=[]
            paramlist2=[]
            
            for column in column_names:
                colval=row[column]
                if colval == False:
                    colval="0"
                elif colval == True:
                    colval="1"
                paramlist.append(colval)
                list2.append("?")
                paramlist2.append(colval)
                
            str1 = ','.join(list2)
            str1='('+str1+')'
            d[ordid].append([str1,paramlist2])
            
            #listval.append(str1)
            #listvalstr=','.join(listval)
            if ctr==0:
              
              qry_str=qry_str+str1
              ctr=ctr+1
            else:
              qry_str=qry_str+" UNION ALL \n VALUES "+str1
            #paramlist.append(tuple(list1))
        paramtup=tuple(paramlist)
        qlist.append([qry_str,paramtup])
    
    dictlist.append(d)
        
        
  return qlist,finaldflist,dictlist,colslistup


# In[ ]:



def prepareData(dfblk,json_exploded_path_lists,colslist,col_len_lists,epochId,tbllist):
    
    
  qlist=[]  
  fullrowslist=[]
    
  dictlist=[]
  colslistup=[]
  badorders=[]
  column_names_list=[]
  column_names_str_list=[]
    
    
  for json_exploded_path_list,romlistfinal1,rom_col_len_list, tbl in zip(json_exploded_path_lists,colslist,col_len_lists,tbllist):
    
        
    finaldf,col_len_list=prepareDFXA(dfblk,json_exploded_path_list,romlistfinal1,rom_col_len_list,epochId,tbl)  
    
    column_names = finaldf.columns
    column_names_str=','.join(column_names)
    column_names_list.append(column_names)
    
    fullrows = finaldf.collect()
    fullrows = list(set(fullrows))
    fullrowslist.append(fullrows)
    
    
    
    for row in fullrows:
            
          orderid=""  
        
          if row:
            
            try:
                
               orderid=row["ORDER_ID"]
            
            except Exception as e:
                
                 print(e)
        
            for column,collen in zip(column_names,col_len_list):
                colval=row[column]
                if( (collen.strip() != 'INTEGER') and (collen != 'NUMERIC (15,2)') and (collen != 'NUMERIC (14,4)') ):
                  if len(str(colval)) > int(collen):
                    badorders.append(orderid)
                elif (collen == 'NUMERIC(15,2)'):
                    if len(str(colval)) > 17:
                        badorders.append(orderid)
                elif (collen == 'NUMERIC(14,4)') :
                    if len(str(colval)) > 18:
                        badorders.append(orderid)
                    
                    
                    
  print("Bad Orders:********************")  
  badorders=list(set(badorders))
    
    
    
  for fullrows,column_names,romlistfinal1,rom_col_len_list,tbl in zip(fullrowslist,column_names_list,colslist,col_len_lists,tbllist):
    
    colslistup.append(column_names)
    column_names_str=','.join(column_names)
    
    qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
    listval=[]
       
    
    lenarr = len(fullrows)
    lenmid = lenarr//2
    
    rowsfirst = fullrows[:lenmid]
    rowssecond = fullrows[lenmid:]
    
    splitrowslist=[]
    splitrowslist.append(rowsfirst)
    splitrowslist.append(rowssecond) 
        
    splitrowslist=split_list(fullrows, 100)
    splitrowslist = [x for x in splitrowslist if x != []]
    
    
    for splitrows in splitrowslist:
        
        
        paramlist=[]
        qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
        ctr=0
        
        
        for row in splitrows:
          #print(row) 
            
          if row:
            
            
           ordid=row["ORDER_ID"]
           if ordid not in badorders:
            list2=[]
            paramlist2=[]
            
            for column in column_names:
                colval=row[column]
                if colval == False:
                    colval="0"
                elif colval == True:
                    colval="1"
                paramlist.append(colval)
                list2.append("?")
                paramlist2.append(colval)
                
            str1 = ','.join(list2)
            str1='('+str1+')'
            
            #listval.append(str1)
            #listvalstr=','.join(listval)
            if ctr==0:
              
              qry_str=qry_str+str1
              ctr=ctr+1
            else:
              qry_str=qry_str+" UNION ALL \n VALUES "+str1
            #paramlist.append(tuple(list1))
        paramtup=tuple(paramlist)
        qlist.append([qry_str,paramtup])
            
        
  return qlist


# In[ ]:
    
   
def dropDupesBadDF(dfbadlists):
    
   ##### Prepare Bad Data Frame ######
   if len(dfbadlists) > 0:
       if len(dfbadlists[0]) > 0:
          dfb = dfbadlists[0][0]
   ctr=0
   for dfbadlist in dfbadlists:
   
     
     if len(dfbadlist) > 0:
         
       if ctr==0:
        #dfb=dfb.select("ORDER_ID","COLUMN_NAME","COLUMN_VALUE","MSG_PARTITION","MSG_OFFSET","MSG_VALUE")
        for dfbd in dfbadlist[1:]:
           #dfbd = dfbd.select("ORDER_ID","COLUMN_NAME","COLUMN_VALUE","MSG_PARTITION","MSG_OFFSET","MSG_VALUE")
              #dfb.show()
              print("^^^^")
              #dfbd.show()
              dfb=dfb.union(dfbd)
           
       else:
           
           for dfbd in dfbadlist:
           #dfbd = dfbd.select("ORDER_ID","COLUMN_NAME","COLUMN_VALUE","MSG_PARTITION","MSG_OFFSET","MSG_VALUE")
               #dfb.show()
               print("%%%%")
               #dfbd.show()
               dfb=dfb.union(dfbd)
       ctr=ctr+1
   if len(dfbadlists) > 0:
       if len(dfbadlists[0]) > 0: 
           dfb=dfb.dropDuplicates()
           print("**************")
           dfb.show()
           print("**************")
   




def prepareData(dfblk,json_exploded_path_lists,colslist,col_len_lists,epochId,tbllist):
    
    
  qlist=[]  
  fullrowslist=[]
    
  dictlist=[]
  colslistup=[]
  badorders=[]
  column_names_list=[]
  column_names_str_list=[]
  
  print("Starting to Detect Bad Orders:........")
  print(datetime.now())   
  dfbadlists=[]
    
  for json_exploded_path_list,romlistfinal1,rom_col_len_list, tbl in zip(json_exploded_path_lists,colslist,col_len_lists,tbllist):
    
       
    finaldf,dfbadlist,col_len_list=prepareDFXA(dfblk,json_exploded_path_list,romlistfinal1,rom_col_len_list,epochId,tbl) 

    dfbadlists.append(dfbadlist)

    print("Starting to Collect Dataframe:........")
    print(datetime.now()) 
    print("Repartition DF...")
    print(datetime.now()) 
    #finaldf=finaldf.coalesce(200)
    print("Repartition DF Complete...")
    print(datetime.now()) 
    finaldf.persist()
    print("Completed caching...")
    print(datetime.now()) 
    column_names = finaldf.columns
    column_names_str=','.join(column_names)
    column_names_list.append(column_names)
  
    print("Completed retrieving Columns...")
    print(datetime.now()) 
    tbl_pkeys=pk_dict.get(tbl)
    #finaldf.dropDuplicates(tbl_pkeys)
    print("Completed Dropping Duplicates...")
    print(datetime.now()) 
    fullrows = finaldf.collect()
    print("Completed collecting dataframe...")
    print(datetime.now()) 
    finaldf.unpersist()
    print("Completed unpersisting...")
    print(datetime.now()) 
    
    print("Ending to Format Data :........")
    print(datetime.now())
    
    
    print("Deduplication Begin:........")
    print(datetime.now())
    fullrows = list(set(fullrows))
    print("Deduplication End:........")
    print(datetime.now())
    
    fullrowslist.append(fullrows)
    
    
    print("Starting to prepare rows...")
    print(datetime.now())
    for row in fullrows:
            
            
          if row:
            orderid=row["ORDER_ID"]
            for column,collen in zip(column_names,col_len_list):
                colval=row[column]
                #print(collen, colval)
                if( (collen.strip() != 'INTEGER') and (collen != 'NUMERIC (15,2)') and (collen != 'NUMERIC (14,4)') ):
                  if len(str(colval)) > int(collen):
                    badorders.append(orderid)
                elif (collen == 'NUMERIC(15,2)' ):
                    if len(str(colval)) > 17:
                        badorders.append(orderid)
                elif (collen == 'NUMERIC(14,4)') :
                    if len(str(colval)) > 18:
                        badorders.append(orderid)
                        
    print("Completing to prepare rows...")
    print(datetime.now())
                    
                    
                    
                    
  print("Bad Orders:********************")  
  badorders=list(set(badorders))
  
  print("Finished Detecting Bad Orders:........")
  print(datetime.now()) 
    
  print("Starting Actual Data Preparation :........")
  print(datetime.now()) 
  
  
  print("Starting to Drop Dupes...")
  print(datetime.now())
  
  dropDupesBadDF(dfbadlists)
  
  print("Completing to Drop Dupes...")
  print(datetime.now())
  
    
  for fullrows,column_names,romlistfinal1,rom_col_len_list,tbl in zip(fullrowslist,column_names_list,colslist,col_len_lists,tbllist):
    
    colslistup.append(column_names)
    column_names_str=','.join(column_names)
    
    qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
    listval=[]
       
    
    lenarr = len(fullrows)
    lenmid = lenarr//2
    
    rowsfirst = fullrows[:lenmid]
    rowssecond = fullrows[lenmid:]
    
    splitrowslist=[]
    splitrowslist.append(rowsfirst)
    splitrowslist.append(rowssecond) 
    
  
        
    splitrowslist=split_list(fullrows,  200)
    splitrowslist = [x for x in splitrowslist if x != []]
    
    # if tbl == 'KAFKARADIAL.ROM_ORDER_LINE_STG0':
    #      splitrowslist=split_list(fullrows,  50)
    #      splitrowslist = [x for x in splitrowslist if x != []]
    
    print("TBL:**********************************")
    print(tbl)
    print(len(splitrowslist))
    
    
    for splitrows in splitrowslist:
        
        
        paramlist=[]
        qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
        ctr=0
        
        
        for row in splitrows:
          #print(row) 
            
          if row:
            
            
           ordid=row["ORDER_ID"]
           if ordid not in badorders:
            list2=[]
            paramlist2=[]
            
            for column in column_names:
                colval=row[column]
                if colval == False:
                    colval="0"
                elif colval == True:
                    colval="1"
                paramlist.append(colval)
                list2.append("?")
                paramlist2.append(colval)
                
            str1 = ','.join(list2)
            str1='('+str1+')'
            
            #listval.append(str1)
            #listvalstr=','.join(listval)
            if ctr==0:
              
              qry_str=qry_str+str1
              ctr=ctr+1
            else:
              qry_str=qry_str+" UNION ALL \n VALUES "+str1
            #paramlist.append(tuple(list1))
        paramtup=tuple(paramlist)
        qlist.append([qry_str,paramtup])
            
        
  return qlist

  print("Ending Actual Data Preparation :........")
  print(datetime.now()) 


# In[ ]:

    
def insertFailedBatch(df,epochId):
    
    tbl="KAFKARADIAL.INSERT_FAILED_BATCH"
    
    fdf=df.select("order.id","partition","offset","value")
    fdf=fdf.withColumnRenamed("id", "ORDER_ID")
    fdf=fdf.withColumnRenamed("partition", "MSG_PARTITION") 
    fdf=fdf.withColumnRenamed("offset", "MSG_OFFSET") 
    fdf=fdf.withColumnRenamed("value", "MSG_VALUE") 
    
    writeToSQLWarehouse(fdf, epochId, tbl)


def prepareDataSingle(dfblk,json_exploded_path_lists,colslist,epochId,tbllist):
    
    
  qlist=[]  
  finaldflist=[]
  dictlist=[]
  colslistup=[]
  
    
  for json_exploded_path_list,romlistfinal1,tbl in zip(json_exploded_path_lists,colslist,tbllist):
    
    finaldf=prepareDFXA(dfblk,json_exploded_path_list,romlistfinal1,epochId,tbl)  
    #finaldflist.append(finaldf)
    column_names = finaldf.columns
    column_names_str=','.join(column_names)
    qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
    listval=[]
    colslistup.append(column_names)
       
    
    
    fullrows = finaldf.collect()
    fullrows = list(set(fullrows))
    
    
    d = defaultdict(list)
    
        
    paramlist=[]
    qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
    ctr=0
        
    for row in fullrows:
            
          if row:
            ordid=row["ORDER_ID"]
            
            list2=[]
            paramlist2=[]
            
            for column in column_names:
                colval=row[column]
                if colval == False:
                    colval="0"
                elif colval == True:
                    colval="1"
                paramlist2.append(colval)
                list2.append("?")
               
                
            str1 = ','.join(list2)
            str1='('+str1+')'
            d[ordid].append([str1,paramlist2])
            
            
            
    dictlist.append(d)
    
        
        
  return dictlist,colslistup


# In[ ]:


def prepareDataSingle(finaldflist,colslist,epochId,tbllist):
    
    
  qlist=[]  
  dictlist=[]
  colslistup=[]
  print(len(finaldflist))
  print(finaldflist)
    
  for finaldf,romlistfinal1,tbl in zip(finaldflist,colslist,tbllist):
    
    column_names = finaldf.columns
    column_names_str=','.join(column_names)
    qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
    listval=[]
    colslistup.append(column_names)
    #finaldf.show()
    
    
    fullrows = finaldf.collect()
    fullrows = list(set(fullrows))
    print(len(fullrows))
    
    d = defaultdict(list)
    
        
    paramlist=[]
    qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
    ctr=0
        
    for row in fullrows:
            
          if row:
            ordid=row["ORDER_ID"]
            
            list2=[]
            paramlist2=[]
            
            for column in column_names:
                colval=row[column]
                if colval == False:
                    colval="0"
                elif colval == True:
                    colval="1"
                paramlist2.append(colval)
                list2.append("?")
               
                
            str1 = ','.join(list2)
            str1='('+str1+')'
            d[ordid].append([str1,paramlist2])
            
            
            
    dictlist.append(d)
    
        
        
  return dictlist,colslistup


# In[ ]:


def insertData(qlist,conn):
    
    
    for q in qlist:
        qry=q[0]
        param=q[1]
        stmt = ibm_db.prepare(conn, qry)
        print("here***")
        print(qry.splitlines()[0])
        #print(type(param))
        #print(param)
        print(datetime.now()) 
        ibm_db.execute(stmt, param)
        print(datetime.now()) 
        print("here####")
    ibm_db.commit(conn)
    


# In[ ]:


def insSingleData(dictlist,tbllist,colslist,conn):
    
  print(datetime.datetime.now())
  #finaldford=finaldflist[0].groupBy("ORDER_ID").count()
  finaldfordlist=dictlist[0].keys()
  print(len(finaldfordlist))
        
  qry=""
  param=""
    
    
  for orderidrow in finaldfordlist:   
    
        
    try:
        
        
      qlist=[]   
    
      for dictelm,cols,tbl in zip(dictlist,colslist,tbllist):
        
        
          column_names_str=','.join(cols)
        
          paramlist=[]
          qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
          ctr=0
    
        
          rowsordlistnew = dictelm[orderidrow]
          #print(len(rowsordlistnew))
          
          for row in rowsordlistnew:
            
            #print(row)
            
            list2=[]
            str1=row[0]
            parampartlist=row[1]
            paramlist=paramlist+parampartlist
            
            if ctr==0:
                
              qry_str=qry_str+str1
              ctr=ctr+1
                
            else:
                
              qry_str=qry_str+" UNION ALL \n VALUES "+str1
            
            #paramlist.append(tuple(list1))
            
          paramtup=tuple(paramlist)
          qlist.append([qry_str,paramtup])
            
    
      insertSingleData(qlist,conn)    
        
    except Exception as e:
            
             print("hereëee....")
             exp_tb=traceback.format_exc()
             print(exp_tb)
             ibm_db.rollback(conn)


# In[ ]:


def prepareSingleDFData(df,json_exploded_path_lists,colslist,epochId,tbllist):
    
    
  qlist=[]  
    
  for json_exploded_path_list,romlistfinal1,tbl in zip(json_exploded_path_lists,colslist,tbllist):
    
        finaldf=prepareDFXA(df,json_exploded_path_list,romlistfinal1,epochId,tbl)  
        column_names = finaldf.columns
        column_names_str=','.join(column_names)
       
        
    
        for row in finaldf.collect():
            list1=[]
            paramlist=[]
            qry_str="INSERT INTO "+tbl+" ("+column_names_str+") VALUES "
            
            for column in column_names:
                list1.append("?")
                paramlist.append(row[column])
            str1 = ','.join(list1)
            str1='('+str1+')'
            qry_str=qry_str+str1
            paramtup=tuple(paramlist)
            qlist.append([qry_str,paramtup])
        
  return qlist


# In[ ]:


def explodeDFJSON(df):
  
      #df=df.withColumn("order", df.value.order)
      #df=df.withColumn("customAttributes", df.value.customAttributes)
      #df=df.withColumn("topicName", df.value.topicName)
      #df=df.drop("value")
      #df.show()
    
      
      for i, row in arraysdf.iterrows():
      
          json_path_to_explode=row['JSON_PATH_TO_EXPLODE']
          json_path_exploded=row['JSON_PATH_EXPLODED']
          print(json_path_to_explode)
          print(json_path_exploded)
      
          df=df.withColumn(json_path_exploded,explode_outer(json_path_to_explode))
          df.show()
          
      return df


# In[ ]:


def updateDF(sdf,tbl):
    
    #sdf.printSchema()
    if tbl=="KAFKARADIAL.ROM_ORDER_LINE_STG0":
       try:
        sdf=sdf.withColumn("IS_ASSOCIATE_DELIVERY", when(col("IS_ASSOCIATE_DELIVERY") == "false",lit("0") )        .when(col("IS_ASSOCIATE_DELIVERY") == "true",lit("1"))         .otherwise(col("IS_ASSOCIATE_DELIVERY")))
       except Exception as e:
         print(e)
    
    return sdf


# In[ ]:


pk_dict = {'ROM_ORDER_HEADER_STG0': ['ORDER_ID'],
           
           'ROM_ORDER_LINE_STG0': ['ORDER_ID', 'ORDER_LINE_ID'],
           
           'ROM_RELATED_ORDERS_STG0' : ['ORDER_ID', 'RELATED_ORDER_ID'],
           
           'ROM_ORDER_PAYMENT_STG0' : ['ORDER_ID', 'TENDER_TYPE', 'PAYMENT_ACCT_NO'],
           
           'ROM_ORDER_PROMOTION_STG0' : ['ORDER_ID', 'ORDER_LINE_ID', 'ORDER_DISCOUNT_ID'],
           
           'ROM_ORDER_REFERENCES_STG0' : ['ORDER_ID', 'REFERENCE_TYPE', 'ATTRIB_NAME', 'ATTRIB_VALUE'],
           
           'ROM_ORDER_TAX_BREAKUP_STG0' : ['ORDER_ID', 'ORDER_LINE_ID', 'ORDER_SUB_LINE_ID', 
                                           'ORDER_CHARGE_ID', 'ORDER_DISCOUNT_ID'],
           
           'ROM_ORDER_LINE_RELATIONSHIP_STG0' : ['ORDER_ID', 'RELATION_TYPE', 
                                                'PARENT_LINE_ID', 'CHILD_LINE_ID'],
           
           'ROM_ORDER_CUSTOMER_INFO_STG0' : ['ORDER_ID', 'CUSTOMER_INFO_ID'],
           
           'ROM_ORDER_LINE_CHARGES_STG0' : ['ORDER_ID', 'ORDER_LINE_ID', 'ORDER_SUB_LINE_ID', 
                                            'ORDER_CHARGE_ID', 'ORDER_DISCOUNT_ID'],
           
           'ROM_ORDER_LINE_STATUS_STG0' : ['ORDER_ID', 'ORDER_LINE_ID', 'ORDER_SUB_LINE_ID',
                                           'STATUS_QTY', 'STATUS_ID']
    
}


v=pk_dict.get('ROM_ORDER_HEADER_STG0')
type(v)


# In[ ]:


###### Prepare Dataframe and Populate the DB #######
    
def prepareDFXA(df,json_exploded_path_list,romlistfinal1,rom_col_len_list,epochId,tbl):
    
       print("&&&&&&&&&&")
       #df.show()
       jsonpathlist_clean=[]
       rom_list_clean = [] 
       romlen_list_clean = []
       dfbadlist=[]
       
       df.registerTempTable("Order_Created")
        
       for elm,colval,collen in zip(json_exploded_path_list,romlistfinal1,rom_col_len_list):
        
           try:
             df.select(elm)                 
             jsonpathlist_clean.append(elm)
             rom_list_clean.append(colval)  
             romlen_list_clean.append(collen)
           except Exception as e:
             #print(e)
             print(elm)
             
             
       for elm,colval,collen in zip(jsonpathlist_clean,rom_list_clean,romlen_list_clean):
        
           try:
               
             if (tbl=="KAFKARADIAL.ROM_ORDER_PAYMENT_STG0"):
                 #df.show()
                 df.select(elm).show()
                 print(length(df.select(elm).collect()[0][0]))  
               
               
             if collen != "INTEGER":  
                 #df=df.withColumn(colval, when(length(col(colval)) > collen,substring(col(colval), 1, collen )        .otherwise(col(colval))))
                  dfbad=df.filter( length(col(elm)) > collen )
                  df=df.filter( length(col(elm).cast("string")) <= collen )
             
                  print("dfgood")
                  #df.show()
                  #dfbad=spark.sql("select * from Order_Created where length(elm) > collen")
                  #dfbad=df.filter( length(col(elm)) > 2 )
                  dfbad=dfbad.select("order.id",col(elm).cast("string"),"partition","offset","value")
                  dfbad=dfbad.withColumnRenamed("id", "ORDER_ID")
                  dfbad=dfbad.withColumnRenamed("partition", "MSG_PARTITION")
                  dfbad=dfbad.withColumnRenamed("offset", "MSG_OFFSET")
                  dfbad=dfbad.withColumnRenamed("value", "MSG_VALUE")
                  elmar=elm.split(".")
                  lastelm=elmar[-1]
                  dfbad=dfbad.withColumnRenamed(lastelm, colval)
                  dfbad=dfbad.withColumn("COLUMN_NAME", lit(colval))
                  #dfbad.show()
                  dfbad=dfbad.withColumnRenamed(colval, "COLUMN_VALUE")
                  print("dfbad")
                  #dfbad.show()
                  dfbadlist.append(dfbad)
                  # dfbad=df.where(length(col(colval)) > collen)
                  # dfbad.select("ORDER_ID",colval,"MSG_PARTITION","MSG_OFFSET","MSG_VALUE")
                  # dfbad=dfbad.withColumn("COLUMN_NAME", colval)
                  # dfbad=dfbad.withColumnRenamed(colval, "COLUMN_VALUE")
                  # dfbadlist.append(dfbad)

           except Exception as e:
             print(e)
             print(colval)
              
       
       cdf=df.select(jsonpathlist_clean)
       #cdf.show()
            
       sdf=cdf.toDF(*rom_list_clean)
       #sdf.show()
         
    
       ######## Update Dataframe ########
       print("Updating")
       sdf = updateDF(sdf,tbl)
       #sdf.show()
    
    
       ##### Remove Duplicates ########
       print("Dropping Duplicates")
       #tbl_pkeys=pk_dict.get(tbl)
       #inaldf=sdf.dropDuplicates(tbl_pkeys)
       finaldf=sdf
       
      
       #writeToSQLWarehouse(dfb,epochId,"INSERT_FAILED_MSG")
       
    
       #finaldf=sdf.dropDuplicates(["ORDER_ID"])
       #finaldf=sdf.withColumn("MERGEDCOL",hash(concat(*rom_list_clean)))
       #finaldf.show()
       #finaldf=finaldf.dropDuplicates(['MERGEDCOL'])
       #finaldf.show()
       #finaldf=finaldf.drop('MERGEDCOL')
       #finaldf.show()
       #finaldf=sdf.distinct()
       #finaldf=sdf.groupBy(rom_list_clean).count.sort().show
       #print(sdf)
       #finaldf.printSchema()
    
       return finaldf,dfbadlist,romlen_list_clean



# In[ ]:

#option("checkpointLocation", "/myapp/GChkPt8")
print(datetime.now())


# In[ ]:
#.option("checkpointLocation", "/myapp/GChkPt7")  

#query1 = df     .writeStream     .outputMode("append")    .option("partition.assignment.strategy", "range")    .foreachBatch(popTablesBlkAtomicNew)    .start()


query1 = df     .writeStream     .outputMode("append")    .option("checkpointLocation", "/myapp/GChkPt11")    .option("partition.assignment.strategy", "range")    .foreachBatch(popTablesBlkAtomicNew)    .start()
    
query1.awaitTermination(600)


# In[ ]:


query1.stop()


# In[ ]:


query1.status




# In[ ]:





# In[ ]:





# In[ ]:




