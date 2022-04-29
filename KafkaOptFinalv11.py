
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
#import ibm_db_sa
from datetime import datetime, date, time
import pyspark.sql.functions as func
from pyspark.sql.functions import broadcast
from pyspark.sql import Window


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
print(argslist)
print(len(argslist))
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
jsonmapfilepath = argslist[1]

with open(jsonmapfilepath) as f:
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



jdbcUrl=""
user=""
password=""
jdbcstr=""
kafka_bootstrap_servers=""
scram_user=""
scram_pass=""
truststore_location=""
truststore_password=""
cp4dcfginfopath = argslist[2]

with open(cp4dcfginfopath) as f:
    
    contents = f.read()
    contents=contents.strip()
    cfg_infolist=contents.split("\n")
    jdbcUrl=cfg_infolist[0]
    user=cfg_infolist[1]
    password=cfg_infolist[2]
    jdbcstr=cfg_infolist[3]
    kafka_bootstrap_servers=cfg_infolist[4]
    scram_user=cfg_infolist[5]
    scram_pass=cfg_infolist[6]
    truststore_location=cfg_infolist[7]
    truststore_password=cfg_infolist[8]
    kafkatopic=cfg_infolist[9]
    
f.close()    



print(jdbcUrl)
print(user)
print(password)
print(jdbcstr)
print(kafka_bootstrap_servers)
print(scram_user)
print(scram_pass)
print(truststore_location)
print(truststore_password)
print(kafkatopic)



# In[ ]:

jsonschemafilepath = argslist[3]
jsondf = spark.read.json(jsonschemafilepath, multiLine=True)
order_created_schema=jsondf.schema


# In[ ]:


jsondf.printSchema()



# In[ ]:

dfi = spark \
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

  #.option("assign", """{"gtest50p":[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49]}""") \


dfi.printSchema()




# In[ ]:
 



def writeToSQLWarehouse(jdf,epochId,tbl):
    
  print(tbl)
  #print(df)
  print("hgjghjgh")
  #jdf.show()
  jdf.write.format("singlestore") \
  .mode("overwrite") \
  .option("ddlEndpoint", jdbcUrl) \
  .option("user", user) \
  .option("password", password) \
  .save(tbl)
  
  print("jhghjgjn")
  #jdf.write.format("jdbc")   .mode("append")   .option("url", jdbcUrl)   .option("dbtable", tbl)   .option("user", user)   .option("password", password)   .save()


# In[ ]:


arraysdf = pd.read_csv("/myapp/arrays_order_created.txt", sep=",")


# In[ ]:


def explodeDF(df):
    
    df=df.withColumn("order", df.mvalue.order)
    df=df.withColumn("customAttributes", df.mvalue.customAttributes)
    df=df.withColumn("topicName", df.mvalue.topicName)
    df=df.drop("mvalue")
    df=df.drop("value")
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



def insertBatch(df,epochId):
    
    tbl="KAFKARADIAL.INSERT_BATCH"
    
    fdf=df.select("mvalue.order.id","partition","offset","value")
    fdf=fdf.withColumnRenamed("id", "ORDER_ID")
    fdf=fdf.withColumnRenamed("partition", "MSG_PARTITION") 
    fdf=fdf.withColumnRenamed("offset", "MSG_OFFSET") 
    fdf=fdf.withColumnRenamed("value", "MSG_VALUE") 
    
    writeToSQLWarehouse(fdf, epochId, tbl)      
    
    

def insertFailedBatch(df,epochId):
    
    tbl="KAFKARADIAL.INSERT_FAILED_BATCH"
    
    fdf=df.select("mvalue.order.id","partition","offset","value")
    fdf=fdf.withColumnRenamed("id", "ORDER_ID")
    fdf=fdf.withColumnRenamed("partition", "MSG_PARTITION") 
    fdf=fdf.withColumnRenamed("offset", "MSG_OFFSET") 
    fdf=fdf.withColumnRenamed("value", "MSG_VALUE") 
    
    writeToSQLWarehouse(fdf, epochId, tbl)    



def prepareData(dfblk,json_exploded_path_lists,colslist,col_len_lists,epochId,tbllist):
    

  finaldflist=[]

  
  print("Starting to Preoare DF List:........")
  print(datetime.now())   
  dfblk,json_exploded_path_lists,colslist,col_len_lists = addGenereicSplFields(dfblk,json_exploded_path_lists,colslist,col_len_lists)
    
  for json_exploded_path_list,romlistfinal1,rom_col_len_list, tbl in zip(json_exploded_path_lists,colslist,col_len_lists,tbllist):
    
    #dfblk.persist()   
    finaldf,col_len_list=prepareDFXA(dfblk,json_exploded_path_list,romlistfinal1,rom_col_len_list,epochId,tbl) 
    #dfblk.unpersist()
    #finaldflist.append(finaldf)
    
    if len(finaldf.dtypes) == 0:
     
       print("Empty")

    else: 
        
       print("******************************")
       print("Non Empty")
       print("******************************") 
       #finaldf.show()
       writeToSQLWarehouse(finaldf, epochId, tbl)
        
  # ordiddf=dfblk.select("order.id")  
  # writeToSQLWarehouse(ordiddf, epochId, tbl)
  
  #return finaldflist

  print("Ending to Prepare DF List:........")
  print(datetime.now()) 

    


# In[ ]:
def popTablesBlkAtomicNew(df, epochId):  #### Bulk Insert
      
      
   #####  Create JDBC Connection #######
   print(datetime.now())
   #df.coalesce(4)
   df.persist()   
   
   
   try:
    
    
    if df.rdd.isEmpty():
       print(df.rdd.isEmpty)
    else:
      #df.show()
      df=df.withColumnRenamed("from_json(CAST(value AS STRING))", "mvalue") 
      #insertBatch(df,epochId)   
    
      if df.rdd.isEmpty():
       print(df.rdd.isEmpty)
    
      else:
    
         ###### Exploding Dataframe #######
         print("Starting to Explode Data:........")
         print(datetime.now()) 
         dfblk=explodeDF(df) 
         #dfblk.show()
         print("Finished Exploding Data:........")
         print(datetime.now()) 
      
          
         ##### Prepare Blk Dataframe & Insert Queries  #######
          
         #qlist,finaldflist=prepareData(dfblk,json_exploded_path_lists,colslist,epochId,tbllist)
         #qlist,finaldflist,dictlist,colslistup=prepareData(dfblk,json_exploded_path_lists,colslist,col_len_lists,epochId,tbllist)
         print("Starting to Prepare Data:........")
         print(datetime.now())  
         #qlist=prepareData(dfblk,json_exploded_path_lists,colslist,col_len_lists,epochId,tbllist)
         prepareData(dfblk,json_exploded_path_lists,colslist,col_len_lists,epochId,tbllist)
         success_df=df.select("mvalue.order.id")
         success_df = success_df.withColumnRenamed("id","ORDER_ID")
         writeToSQLWarehouse(success_df, epochId, "KAFKARADIAL.SUCCESS_ORDERS")
         #print(xyz)
         print("Finishing to Prepare Data:.......")
    
         #print(qlist)
         print("*****")
   
       
    
   except Exception as e:
          print("here...........")
          print(e)
          exp_tb=traceback.format_exc()
          #print(expk)
          insertFailedBatch(df,epochId)
          #popTablesAtomicUpdated(df, epochId, conn)
          
   finally:
          print("Complete")
          df.unpersist()
          #  if conn==True:
          #    #print(conn)
          #    ibm_db.close(conn)




# In[ ]:
    
    
def addGenereicSplFields(df,json_exploded_path_lists,colslist,col_len_lists):
    
    try:
    
        df = df.withColumn("DW_SOURCE_ID" , concat(col("topicName"), lit("-"), lit(datetime.now()), lit("-"), col("partition"), lit("-"), col("offset") ))
        for json_exploded_path_list,romlistfinal1,rom_col_len_list in zip(json_exploded_path_lists,colslist,col_len_lists):
            json_exploded_path_list.append("DW_SOURCE_ID")
            romlistfinal1.append("DW_SOURCE_ID")
            rom_col_len_list.append("100000000")  
        
    except Exception as e:
        
        print(" Can't obtain DW_SOURCE_ID ")
        print(e) 
    

    return df,json_exploded_path_lists,colslist,col_len_lists   


def updateDF(sdf,tbl):
    
    #sdf.printSchema()
    if tbl=="KAFKARADIAL.ROM_ORDER_LINE_STG0":
       try:
        sdf=sdf.withColumn("IS_ASSOCIATE_DELIVERY", when(col("IS_ASSOCIATE_DELIVERY") == "false",lit("0") )        .when(col("IS_ASSOCIATE_DELIVERY") == "true",lit("1"))         .otherwise(col("IS_ASSOCIATE_DELIVERY")))
       except Exception as e:
         print(e)
    
    return sdf


# In[ ]:
    
############################  TABLE UPDATES  #############################   



def updateDFSplFields_ROM_ORDER_HEADER_STG0(df, jsonpathlist_clean, rom_list_clean, romlen_list_clean):
    
    
        try:
        
            df = df.withColumn("ORDER_HEADER_KEY", concat(col("order.sellerId"), lit("-"), col("order.id")))
            jsonpathlist_clean.append("ORDER_HEADER_KEY")
            rom_list_clean.append("ORDER_HEADER_KEY")
            romlen_list_clean.append("1000000")
            
        except Exception as e:
            
             print(" Can't obtain ORDER_HEADER_KEY ")
             print(e)
        
        
        try: 
    
           df.select("order.hfrNumber")
           #df=df.withColumn("HFR_NUMBER" , col("order.hfrNumber") )
           jsonpathlist_clean.append("order.hfrNumber")
           rom_list_clean.append("HFR_NUMBER")
           romlen_list_clean.append("100000") 

        except Exception as e:
    
            try:
        
                df.select("order.raNumber")
                #df=df.withColumn("HFR_NUMBER" , col("order.raNumber") )
                jsonpathlist_clean.append("order.raNumber")
                rom_list_clean.append("HFR_NUMBER")
                romlen_list_clean.append("100000")
        
            except Exception as e:
        
                print(" HFR NUMBER order.hfrNumber or order.raNumber NOT PRESENT ")
            
            
        try: 
    
           df.select("order.billingAddressRefId")
           #df=df.withColumn("BILL_TO_KEY" , col("order.billingAddressRefId") )
           jsonpathlist_clean.append("order.billingAddressRefId")
           rom_list_clean.append("BILL_TO_KEY")
           romlen_list_clean.append("100000") 

        except Exception as e:
        
            print(" BILL_TO_KEY -->  order.billingAddressRefId NOT PRESENT ")
            
            
        return df, jsonpathlist_clean, rom_list_clean,romlen_list_clean
    
    


def updateDFSplFields_ROM_ORDER_LINE_STG0(df, jsonpathlist_clean,rom_list_clean,romlen_list_clean ):
    
        try: 
            
           #df2 = df.groupBy(col("order.id"), col("order_lineItems"),col("order_lineItems_sublineItems")).count()
           #df2 = df2.coalesce(8)
           df2 = df2.groupBy("id","order_lineItems").agg(sum('order_lineItems_sublineItems.quantity').alias("ORDERED_QTY"))
           #df = df.withColumn("ORDERED_QTY", func.sum("order_lineItems_sublineItems.quantity") \
           #                          .over(Window.partitionBy("order_lineItems"))) 
           #df2.select("*","order_lineItems_sublineItems.quantity").show()
           df=df.alias('a').join(broadcast(df2.alias('b')),col('b.id') == col('a.order.id'),"left").select("a.*","b.ORDERED_QTY")
            
           jsonpathlist_clean.append("ORDERED_QTY")
           rom_list_clean.append("ORDERED_QTY")
           romlen_list_clean.append("100000") 
            
        except Exception as e:
            print(e)
            try:
                df.select("order_lineItems.quantity")
                
                #df = df.groupBy(col("order.id"), col("order_lineItems.quantity")).sum("order_lineItems.quantity")
                #df = df.withColumnRenamed("sum(order_lineItems.quantity AS `quantity`)", "ORDERED_QTY") 
                jsonpathlist_clean.append("order_lineItems.quantity")
                rom_list_clean.append("ORDERED_QTY")
                romlen_list_clean.append("10000")
                
            except Exception as e:    
                print(e)
                print(" ORDERED_QTY not present ")
                
                 
        try: 
           print("********")
           #df.show()
           df.select("order_lineItems.shippingAddressRefId")
           #df=df.withColumn("SHIP_TO_KEY" , col("order_lineItems.shippingAddressRefId") )
           jsonpathlist_clean.append("order_lineItems.shippingAddressRefId")
           rom_list_clean.append("SHIP_TO_KEY")
           romlen_list_clean.append("100000") 

        except Exception as e:
            print(e)
            print(" SHIP_TO_KEY -->  order_lineItems.shippingAddressRefId NOT PRESENT ")
            
    
        return df, jsonpathlist_clean,rom_list_clean,romlen_list_clean
        




def updateDFSplFields_ROM_RELATED_ORDERS_STG0( df,jsonpathlist_clean,rom_list_clean,romlen_list_clean ):

    try: 
            df.select("order.id","order.sellerId","order_relatedOrders.id","order_relatedOrders_relatedLines.lineNo","order_relatedOrders_relatedLines.originalLineNo" )
            df = df.withColumn("RELATED_ORDER_KEY", concat(col("order.id"), lit("-"), col("order.sellerId"), 
                 lit("-"),  col("order_relatedOrders.id"),  lit("-"), col("order_relatedOrders_relatedLines.lineNo"),
                 lit("-"),  col("order_relatedOrders_relatedLines.originalLineNo")    ))
            
            jsonpathlist_clean.append("RELATED_ORDER_KEY")
            rom_list_clean.append("RELATED_ORDER_KEY")
            romlen_list_clean.append("100000")
            
    except Exception as e:    
                jsonpathlist_clean=[]
                rom_list_clean=[] 
                romlen_list_clean=[]
                print(" RELATED_ORDER_KEY not present ")
                
                
    return df,jsonpathlist_clean,rom_list_clean,romlen_list_clean




def updateDFSplFields_ROM_ORDER_PAYMENT_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean):


         try:
            
            
            df.select("order_paymentMethods.code","order_paymentMethods.creditCardNumber")     
            df=df.withColumn("PAYMENT_ACCT_NO", when(col("order_paymentMethods.code") == "CREDIT_CARD",col("order_paymentMethods.creditCardNumber") )      
                     .otherwise(None))
            
         except Exception as e:
            
            print(e)

         try:
            
            
            df.select("order_paymentMethods.code","order_paymentMethods.payPalNumber")     
            df=df.withColumn("PAYMENT_ACCT_NO", when(col("order_paymentMethods.code") == "PAYPAL",col("order_paymentMethods.payPalNumber") )      
                     .otherwise("PAYMENT_ACCT_NO"))
            
         except Exception as e:
            
            print(e)
            
            
         try:
            
            
            df.select("order_paymentMethods.code","order_paymentMethods.storedValueCardNumber")     
            df=df.withColumn("PAYMENT_ACCT_NO", when(col("order_paymentMethods.code") == "STORED_VALUE_CARD",col("order_paymentMethods.storedValueCardNumber") )      
                     .otherwise("PAYMENT_ACCT_NO"))
            
         except Exception as e:
            
            print(e)
            
            
         try:
            
            
            df.select("order_paymentMethods.code","order_paymentMethods.storedValueCardNumber")     
            df=df.withColumn("PAYMENT_ACCT_NO", when(col("order_paymentMethods.code") == "PREPAID_CARD",None)
                 .otherwise("PAYMENT_ACCT_NO"))
            
         except Exception as e:
            
            print(e)
        
        
        
        #try:
            
            
            #df = df.withColumn("PAYMENT_ACCT_NO", when(col("order_paymentMethods.code") == "CREDIT_CARD",col("order_paymentMethods.creditCardNumber") )      
                     #.when(col("order_paymentMethods.code") == "PAYPAL",col("order_paymentMethods.payPalNumber")) 
                     #.when(col("order_paymentMethods.code") == "STORED_VALUE_CARD",col("order_paymentMethods.storedValueCardNumber"))
                     #.when(col("order_paymentMethods.code") == "PREPAID_CARD",None )
                     #.otherwise(None))
            
        #except Exception as e:
            
            #print("PAYMENT_ACCT_NO not found")


         try:
        
           ## OrderId + tenderType + AccountNo  ## AccountNo - verify Mapping 
           df = df.withColumn("PAYMENT_KEY", concat_ws('',col("order.id"), lit("-"), col("order_paymentMethods.tenderType"), 
                            lit("-"),  col("PAYMENT_ACCT_NO")))
                            
           jsonpathlist_clean.append("PAYMENT_KEY")
           rom_list_clean.append("PAYMENT_KEY")
           romlen_list_clean.append("100000")  
           
           #df.select("order_paymentMethods.code","PAYMENT_KEY").show() 
         except Exception as e:
              
            print("PAYMENT_KEY not found")
            print(e)   
                                                    
                                                    
                                                    
         return df, jsonpathlist_clean,rom_list_clean,romlen_list_clean
                                     
                                     
    
def updateDFSplFields_ROM_ORDER_PROMOTION_STG0( df,jsonpathlist_clean,rom_list_clean,romlen_list_clean ):


       try:   
            
           df.select("order.id","order_lineItems.lineNo","order_lineItems_sublineItems.sublineNo","order_lineItems_charges.id","order_lineItems_charges_discounts.id")                                          
           ### OrderId + LineNo + SubLineNo + ChargeId + DiscountsId
           df = df.withColumn("PROMOTION_KEY", concat(col("order.id"), lit("-"), col("order_lineItems.lineNo"), 
                 lit("-"),  col("order_lineItems_sublineItems.sublineNo"),  lit("-"), col("order_lineItems_charges.id"),
                 lit("-"),  col("order_lineItems_charges_discounts.id")    ))  
                                                    
           jsonpathlist_clean.append("PROMOTION_KEY")
           rom_list_clean.append("PROMOTION_KEY")    
           romlen_list_clean.append("100000") 
                                                    
       except Exception as e:
            jsonpathlist_clean=[]
            rom_list_clean=[] 
            print(e) 
            print("PROMOTION_KEY not found")
            
                                   
       #print(jsonpathlist_clean)  
    
                                                    
       if "PROMOTION_KEY" in jsonpathlist_clean: 

        
        try: 
            
           df.select("*","order_lineItems_charges_discounts.customAttributes.name","order_lineItems_charges_discounts.customAttributes.value") 

                                                    
           try:
            
                df.select(col("order_lineItems_charges_discounts.customAttributes.name").getItem(0))
                df.select(col("order_lineItems_charges_discounts.customAttributes.value").getItem(0))
                df.select(col("order_lineItems_charges_discounts.customAttributes.name").getItem(1))
                df.select(col("order_lineItems_charges_discounts.customAttributes.value").getItem(1))
                df.select(col("order_lineItems_charges_discounts.customAttributes.name").getItem(2))
                df.select(col("order_lineItems_charges_discounts.customAttributes.value").getItem(2))
                
                df=df.withColumn("PROMOTION_ID",when(df["order_lineItems_charges_discounts.customAttributes.name"].getItem(0) == "promoCode",
                                                df["order_lineItems_charges_discounts.customAttributes.value"].getItem(0))).withColumn("PROMOTION_DESCRIPTION",
                                 when(df["order_lineItems_charges_discounts.customAttributes.name"].getItem(1) == "promoDescription",
                                 df["order_lineItems_charges_discounts.customAttributes.value"].getItem(1))).withColumn("PROMOTION_CODE",
                                 when(df["order_lineItems_charges_discounts.customAttributes.name"].getItem(2) == "promoId",
                                 df["order_lineItems_charges_discounts.customAttributes.value"].getItem(2)))
                                                                                                                        
                                                                                                                        
                                                                                                                        #.withColumn("PROMOTION_DESCRIPTION",
                                 #when(df["order_lineItems_charges_discounts.customAttributes.name"].getItem(3) == "EffectType",
                                 #df["order_lineItems_charges_discounts.customAttributes.value"].getItem(3)))


                jsonpathlist_clean.append("PROMOTION_ID")
                rom_list_clean.append("PROMOTION_ID")    
                romlen_list_clean.append("100000") 
                
                jsonpathlist_clean.append("PROMOTION_DESCRIPTION")
                rom_list_clean.append("PROMOTION_DESCRIPTION")    
                romlen_list_clean.append("100000") 
                
                jsonpathlist_clean.append("PROMOTION_CODE")
                rom_list_clean.append("PROMOTION_CODE")    
                romlen_list_clean.append("100000") 
            
           except Exception as e:
               
               print("PROMOTION details not retrieved")
               #print(e)
                                                    
                                                    
        except Exception as e:
               
               print(" Can't retrieve Promotion Custom Attributes")
               #print(e)
                                                    

                    
       return df,jsonpathlist_clean,rom_list_clean,romlen_list_clean       

    
         
def updateDFSplFields_ROM_ORDER_REFERENCES_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean):
    
    
           dft = spark.createDataFrame([], StructType([]))
           dft1 = spark.createDataFrame([], StructType([]))
           dft2 = spark.createDataFrame([], StructType([]))
           dft3 = spark.createDataFrame([], StructType([]))
           dft4 = spark.createDataFrame([], StructType([]))
            
           dfarr=[]
           
           
           print("HERE......................................................")
           
    
           try:

               
               dfexp = df.withColumn("order_customAttributes",explode("order.customAttributes"))
                
               if dfexp.rdd.isEmpty():
                   
                   print("ORDER Custom Attributes Empty")
                   print(dfexp.rdd.isEmpty)
                    
               else:
                
                   dft = dfexp.withColumn("REFERENCE_TYPE", lit("ORDER"))
                   dft = dft.withColumn("REFERENCE_KEY", col("order.id"))
                   dft = dft.withColumn("ATTRIB_NAME", col("order_customAttributes.name"))
                   dft = dft.withColumn("ATTRIB_VALUE", col("order_customAttributes.value"))
                   dft = dft.drop("order_customAttributes")
                   dfarr.append(dft)
                    
                   print("AT ORDER")
                
                
                                                    
           except Exception as e:
                                                    
                  print(e)  
                              
                        
           try:
            
               dfexp = df.withColumn("order_charges_customAttributes",explode("order_charges.customAttributes"))
                
               if dfexp.rdd.isEmpty():
                   
                   print("CHARGES Custom Attributes Empty")
                   print(dfexp.rdd.isEmpty)
                    
               else:
                
                   dft1 = dfexp.withColumn("REFERENCE_TYPE", lit("CHARGES"))
                   dft1 = dft1.withColumn("REFERENCE_KEY", col("order.id"))
                   dft1 = dft1.withColumn("ATTRIB_NAME", col("order_charges_customAttributes.name"))
                   dft1 = dft1.withColumn("ATTRIB_VALUE", col("order_charges_customAttributes.value")) 
                   dft1 = dft1.drop("order_charges_customAttributes")
                   dfarr.append(dft1)
                    
                   print("AT CHARGES")
                                                                    
           except Exception as e:
                  
                  print("Can't find CHARGES")
                  #print(e)
                                                    
           try:
                                                    
               dfexp = df.withColumn("order_lineItems_customAttributes",explode("order_lineItems.customAttributes"))
                
               if dfexp.rdd.isEmpty():
                  
                   print("LINE Custom Attributes Empty")
                   print(dfexp.rdd.isEmpty)
                    
               else:
                
                   dft2 = dfexp.withColumn("REFERENCE_TYPE", lit("LINE"))
                   dft2 = dft2.withColumn("REFERENCE_KEY", col("order.id"))
                   dft2 = dft2.withColumn("ATTRIB_NAME", col("order_lineItems_customAttributes.name"))
                   dft2 = dft2.withColumn("ATTRIB_VALUE", col("order_lineItems_customAttributes.value")) 
                   dft2 = dft2.drop("order_lineItems_customAttributes")
                   dfarr.append(dft2)
                   print("********")
                   print(dft2.columns) 
                   print("AT LINE")
                   print("********")
                
                                                    
           except Exception as e:
                  
                  print("Can't find LINE")
                  #print(e)
                                                    
            
           try:
            
               dfexp = df.withColumn("order_lineItems_charges_customAttributes",explode("order_lineItems_charges.customAttributes"))
            
               if dfexp.rdd.isEmpty():
                   
                   print("LINE_CHARGES Custom Attributes Empty")
                   print(dfexp.rdd.isEmpty)
                    
               else:
                  
                   dft3=dfexp.withColumn("REFERENCE_TYPE", lit("LINE_CHARGES"))
                   dft3 = dft3.withColumn("REFERENCE_KEY", col("order.id"))
                   dft3 = dft3.withColumn("ATTRIB_NAME", col("order_lineItems_charges_customAttributes.name"))
                   dft3 = dft3.withColumn("ATTRIB_VALUE", col("order_lineItems_charges_customAttributes.value")) 
                   dft3 = dft3.drop("order_lineItems_charges_customAttributes")
                   dfarr.append(dft3)
                    
                   print("AT LINE_CHARGES") 
               
                                                    
           except Exception as e:
            
                  print("Can't find LINE_CHARGES")
                                                    
                  #print(e)
                                                    
            
           try:

               dfexp = df.withColumn("order_paymentMethods_customAttributes",explode("order_paymentMethods.customAttributes"))
            
               if dfexp.rdd.isEmpty():
                   
                   print("PAYMENT Custom Attributes Empty")
                   print(dfexp.rdd.isEmpty)
                    
               else:
                
                   dft4=dfexp.withColumn("REFERENCE_TYPE", lit("PAYMENT"))
                   dft4 = dft4.withColumn("REFERENCE_KEY", col("order.id"))
                   dft4 = dft4.withColumn("ATTRIB_NAME", col("order_paymentMethods_customAttributes.name"))
                   dft4 = dft4.withColumn("ATTRIB_VALUE", col("order_paymentMethods_customAttributes.value")) 
                   dft4 = dft4.drop("order_paymentMethods_customAttributes")
                   dfarr.append(dft4)
                    
                   print("AT PAYMENT") 
                    
                                                    
           except Exception as e:
                  
                  print("Can't find PAYMENT")
                  #print(e)    
                    
                    
           try: 
               
               print("Length of DFARR...........")
               print(len(dfarr))
               if dfarr:
                 
                  for dfe in dfarr[1:]:
                    
                        dfe.select("REFERENCE_TYPE", "REFERENCE_KEY").show(2)
                        
                        print(dft.columns)
                        print("==========================")
                        print(dfe.columns)
                    
                        dft = dft.union(dfe)
                
                  df=dft
                    
                  jsonpathlist_clean.append("REFERENCE_KEY")
                  rom_list_clean.append("REFERENCE_KEY")    
                  romlen_list_clean.append("100000")   
                
                  jsonpathlist_clean.append("REFERENCE_TYPE")
                  rom_list_clean.append("REFERENCE_TYPE")    
                  romlen_list_clean.append("100000")
                  
                  jsonpathlist_clean.append("ATTRIB_NAME")
                  rom_list_clean.append("ATTRIB_NAME")    
                  romlen_list_clean.append("100000")   
                
                  jsonpathlist_clean.append("ATTRIB_VALUE")
                  rom_list_clean.append("ATTRIB_VALUE")    
                  romlen_list_clean.append("100000")
                    
           except Exception as e:
                  
                  print("Error during union")                                  
                  print(e)
                    
                
           return df,jsonpathlist_clean,rom_list_clean,romlen_list_clean
                                                    

def updateDFSplFields_ROM_ORDER_TAX_BREAKUP_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean):
    
    try:                                            
                                                    
           ### TAX_BREAKUP_KEY: OrderId + LineNo + SubLineNo + ChargeId + DiscountsId + TaxSequence 
           ### Need to check on Tax Sequence.   
           #df.select("order.id","order_lineItems.lineNo","order_lineItems_sublineItems.sublineNo","order_lineItems_charges.id","order_lineItems_charges_discounts.id","TaxSequence")
           df.select("order.id","order_lineItems.lineNo","order_lineItems_sublineItems.sublineNo","order_lineItems_charges.id","order_lineItems_charges_discounts.id")

           #df = df.withColumn("TAX_BREAKUP_KEY", concat(col("order.id"), lit("-"), col("order_lineItems.lineNo"), 
           #      lit("-"),  col("order_lineItems_sublineItems.sublineNo"),  lit("-"), col("order_lineItems_charges.id"),
           #      lit("-"),  col("order_lineItems_charges_discounts.id"), lit("-"),col("TaxSequence")    ))  
           df = df.withColumn("TAX_BREAKUP_KEY", concat(col("order.id"), lit("-"), col("order_lineItems.lineNo"), 
                 lit("-"),  col("order_lineItems_sublineItems.sublineNo"),  lit("-"), col("order_lineItems_charges.id"),
                 lit("-"),  col("order_lineItems_charges_discounts.id") ))                                          
           jsonpathlist_clean.append("TAX_BREAKUP_KEY")
           rom_list_clean.append("TAX_BREAKUP_KEY")  
           romlen_list_clean.append("100000") 
                                                    
    except Exception as e:
              
            print("TAX_BREAKUP_KEY not found")
            #print(e)   
            
            
    return df,jsonpathlist_clean,rom_list_clean,romlen_list_clean


    
def updateDFSplFields_ROM_ORDER_LINE_RELATIONSHIP_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean):
    
    try:                                                 
             
           ### RELATED_ORDER_KEY: SellerId + OrderId + RelType + ParentLineNo + ChileLineNo
           df.select("order.sellerId","order.id","order_relatedOrders.type","order_lineItems.lineNo","order_relatedOrders_relatedLines.lineNo")
           df = df.withColumn("RELATED_ORDER_KEY", concat(col("order.sellerId"), lit("-"), col("order.id"), 
                 lit("-"),  col("order_relatedOrders.type"),  lit("-"), col("order_lineItems.lineNo"),
                 lit("-"),  col("order_relatedOrders_relatedLines.lineNo") ))  
                                                    
           jsonpathlist_clean.append("RELATED_ORDER_KEY")
           rom_list_clean.append("RELATED_ORDER_KEY")    
           romlen_list_clean.append("100000") 
                                                    
    except Exception as e:
            jsonpathlist_clean = []
            rom_list_clean = []
            romlen_list_clean = []
            print("RELATED_ORDER_KEY not found")
            #print(e)  
            
    return df, jsonpathlist_clean,rom_list_clean,romlen_list_clean



def updateDFSplFields_ROM_ORDER_LINE_CHARGES_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean):
    
           try:   
                                                    
            ### LINE_CHARGES_KEY:  OrderId + LineNo + SubLineNo + ChargeId + DiscountsId
            df.select("order.id","order_lineItems.lineNo","order_lineItems_sublineItems.sublineNo","order_lineItems_charges.id","order_lineItems_charges_discounts.id")
            
            df = df.withColumn("LINE_CHARGES_KEY", concat(col("order.id"), lit("-"),  col("order_lineItems.lineNo"),
                 lit("-"),  col("order_lineItems_sublineItems.sublineNo"),
                 lit("-"),  col("order_lineItems_charges.id"),
                 lit("-"),  col("order_lineItems_charges_discounts.id") ))  
                                                    
            jsonpathlist_clean.append("LINE_CHARGES_KEY")
            rom_list_clean.append("LINE_CHARGES_KEY")
            romlen_list_clean.append("0")
                                                    
           except Exception as e:
                                                    
                 print(e)    
                                                    
           
           try:
                
                df.withColumn("order_lineItems_charges_customAttributes", explode_outer("order_lineItems_charges.customAttributes"))  
                df=df.withColumn("order_lineItems_charges_customAttributes", explode_outer("order_lineItems_charges.customAttributes"))  
                df.select("*",
                          "order_lineItems_charges_customAttributes.name","order_lineItems_charges_customAttributes.value")
                df=df.select("*",
                     "order_lineItems_charges_customAttributes.name","order_lineItems_charges_customAttributes.value")
                df=df.withColumnRenamed("name","CHARGES_CUST_ATTR_NAME")  
                df=df.withColumnRenamed("value","CHARGES_CUST_ATTR_VALUE")  
              
                df=df.withColumn("REFERENCE", when(col("CHARGES_CUST_ATTR_NAME") == "Reference", col("CHARGES_CUST_ATTR_VALUE") ) )
                df=df.drop("CHARGES_CUST_ATTR_NAME")
                df=df.drop("CHARGES_CUST_ATTR_VALUE")
               
                jsonpathlist_clean.append("REFERENCE")
                rom_list_clean.append("REFERENCE")
                romlen_list_clean.append("100000") 
            
           except Exception as e:
                  print("REFERENCE ERROR...............")
                                                    
                  print(e)   
                                                    
           try:

               df.select("order_lineItems_charges.detail.originalChargeAmount")
               #df=df.withColumn("ORIGINAL_CHARGE_AMT", col("order_lineItems_charges.detail.originalChargeAmount") ) 
            
               jsonpathlist_clean.append("order_lineItems_charges.detail.originalChargeAmount")
               rom_list_clean.append("ORIGINAL_CHARGE_AMT")
               romlen_list_clean.append("100000") 
      
           except Exception as e:
                         
                 try:
                      
                     df.select("order_lineItems_charges.detail.amount")
                     #df=df.withColumn("ORIGINAL_CHARGE_AMT", col("order_lineItems_charges.detail.amount") ) 
                     jsonpathlist_clean.append("order_lineItems_charges.detail.amount")
                     rom_list_clean.append("ORIGINAL_CHARGE_AMT")
                     romlen_list_clean.append("100000") 

                 except Exception as e:
                           
                           print("ORIGINAL_CHARGE_AMT not found....")                         
                           print(e)
                                                    


           return df, jsonpathlist_clean,rom_list_clean,romlen_list_clean




def updateDFSplFields_ROM_ORDER_LINE_STATUS_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean):
    
    
       try:

          df.select("order.sellerId", "order.id", "order_lineItems_sublineItems.sublineNo" )
          df = df.withColumn("ORDER_LINE_STATUS_KEY", concat(col("order.sellerId"), lit("-"),  col("order.id"),
                 lit("-"),  col("order_lineItems_sublineItems.sublineNo") ))                                           
                                                    
          jsonpathlist_clean.append("ORDER_LINE_STATUS_KEY")
          rom_list_clean.append("ORDER_LINE_STATUS_KEY") 
          romlen_list_clean.append("100000")  
          
          
       except Exception as e:
                           
                           print("ORDER_LINE_STATUS_KEY not found....")                         
                           print(e)
            
            
       return df, jsonpathlist_clean,rom_list_clean,romlen_list_clean
    


    
######## Update Special Fields in the Tables #########    
    
def updateDFSplFields(df,tbl,jsonpathlist_clean,rom_list_clean,romlen_list_clean):
     
    
    
    if ( tbl ==  "KAFKARADIAL.ROM_ORDER_HEADER_STG0" ):
        
       print("Accessing Special Fields from "+tbl) 
        
       df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields_ROM_ORDER_HEADER_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean)
                    
        
    elif ( tbl == "KAFKARADIAL.ROM_ORDER_LINE_STG0" ):
        
        print("Accessing Special Fields from "+tbl) 
        
        df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields_ROM_ORDER_LINE_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean)
        
        
    elif ( tbl == "KAFKARADIAL.ROM_RELATED_ORDERS_STG0" ):
        
        print("Accessing Special Fields from "+tbl) 
        
        df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields_ROM_RELATED_ORDERS_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean)
                
        
    elif ( tbl == "KAFKARADIAL.ROM_ORDER_PAYMENT_STG0" ): 
        
        print("Accessing Special Fields from "+tbl) 
        
        df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields_ROM_ORDER_PAYMENT_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean)                              
                                                         
    
    elif ( tbl == "KAFKARADIAL.ROM_ORDER_PROMOTION_STG0" ):  
        
        print("Accessing Special Fields from "+tbl) 
        
        df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields_ROM_ORDER_PROMOTION_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean)
                                                    
    
    elif ( tbl == "KAFKARADIAL.ROM_ORDER_REFERENCES_STG0" ): 
        
        print("Accessing Special Fields from "+tbl) 
                                                    
        df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields_ROM_ORDER_REFERENCES_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean)
                                                    
                                                 
    elif ( tbl == "KAFKARADIAL.ROM_ORDER_TAX_BREAKUP_STG0" ):
        
        print("Accessing Special Fields from "+tbl) 
                                                 
        df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields_ROM_ORDER_TAX_BREAKUP_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean)                                        
                                                    
                                                 
    elif ( tbl == "KAFKARADIAL.ROM_ORDER_LINE_RELATIONSHIP_STG0" ):
        
         print("Accessing Special Fields from "+tbl) 
         
         df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields_ROM_ORDER_LINE_RELATIONSHIP_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean)                                    
                                                 
    elif ( tbl == "KAFKARADIAL.ROM_ORDER_CUSTOMER_INFO_STG0" ):
        
            print("Accessing Special Fields from "+tbl) 
            print("No Special Fields")
                                                 
    elif  ( tbl == "KAFKARADIAL.ROM_ORDER_LINE_CHARGES_STG0" ):  
        
          print("Accessing Special Fields from "+tbl) 
                                                    
          df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields_ROM_ORDER_LINE_CHARGES_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean)
                                                 
    elif  ( tbl == "KAFKARADIAL.ROM_ORDER_LINE_STATUS_STG0" ):
        
          print("Accessing Special Fields from "+tbl) 
                                                 
          df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields_ROM_ORDER_LINE_STATUS_STG0(df,jsonpathlist_clean,rom_list_clean,romlen_list_clean)                                      
                                                 
                                                 
    return df,jsonpathlist_clean,rom_list_clean,romlen_list_clean     
        


###### Prepare Dataframe and Populate the DB #######
 
    
def prepareDFXA(df,json_exploded_path_list,romlistfinal1,rom_col_len_list,epochId,tbl):
    
       print("&&&&&&&&&&")
       #dfcnt=df.count()
       #print(dfcnt)
       print("&&&&&&&&&&")
       jsonpathlist_clean=[]
       rom_list_clean = [] 
       romlen_list_clean = []
       dfbadlist=[]
       
       
        
       for elm,colval,collen in zip(json_exploded_path_list,romlistfinal1,rom_col_len_list):
        
           try:
             df.select(elm)
             jsonpathlist_clean.append(elm)
             rom_list_clean.append(colval)  
             romlen_list_clean.append(collen)
           except Exception as e:
             #print(e)
             print(elm)
             
             
       # for elm,colval,collen in zip(jsonpathlist_clean,rom_list_clean,romlen_list_clean):
        
       #     try:
               
       #       df=df.withColumn(colval, when(length(col(colval)) > collen,substring(col(colval), 1, collen )        .otherwise(col(colval))))


       #     except Exception as e:
       #       #print(e)
       #       print(colval)
             
       print(rom_list_clean)  
       
       # print("Checking Counts......................................")
       
       # print(df.count())
       
       # print("Checking Counts......................................")
             
       df,jsonpathlist_clean,rom_list_clean,romlen_list_clean = updateDFSplFields(df,tbl,jsonpathlist_clean,rom_list_clean, romlen_list_clean)      
      
       #print("Checking Counts after Update......................................")
       
       #print(df.count())
       
       #print("Checking Counts after Update......................................")  
      

       cdf=df.select(jsonpathlist_clean)
       
        
       sdf=cdf.toDF(*rom_list_clean)
         
    
       ######## Update Dataframe ########
       print("Updating")
       sdf = updateDF(sdf,tbl)
    
    
       ##### Remove Duplicates ########
       print("Dropping Duplicates")
       #tbl_pkeys=pk_dict.get(tbl)
       #finaldf = sdf.groupby(rom_list_clean).count()
       finaldf = sdf.groupby(rom_list_clean).count()
       finaldf=finaldf.drop("count")
     
        
        
       finaldf = finaldf.coalesce(16)
       
           
       # if tbl == "KAFKARADIAL.ROM_ORDER_TAX_BREAKUP_STG0":
                       
       #      finaldf = finaldf.withColumn("TAX_PERCENTAGE", func.round(finaldf["TAX_PERCENTAGE"], 2))
        
      
    
       return finaldf,romlen_list_clean




# In[ ]:

print(datetime.now())


# In[ ]:

query1 = dfi     .writeStream     .outputMode("append")    .option("checkpointLocation", "/myapp/GChkPt11")    .option("partition.assignment.strategy", "range")    .foreachBatch(popTablesBlkAtomicNew)    .start()
    
query1.awaitTermination(50)


# In[ ]:

while ( (query1.status['isDataAvailable'] == True) and (query1.status['isTriggerActive'] == True) ):
    
    query1.status
    
query1.stop()


# In[ ]:


query1.status




# In[ ]:
