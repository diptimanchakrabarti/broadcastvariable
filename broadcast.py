import pyspark
#import parseYaml
from pyspark import SparkContext, SparkConf
import argparse
import sys
import os
import glob
import ast
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import trim
from pyspark.sql.types import *
import pyspark.sql.functions  as sf
from pyspark.sql.types import *
#import parseYaml_new_json


#import pymongo
#from pymongo import MongoClient
import numpy as np
#import commonMongoDB
#import pandas as pd
#import #pickledb
#import logError_json



def main():
    arguments=sys.argv
    srcFile=arguments[1]
    lookup=arguments[2]
    trgtFilePath=arguments[3]
    trgtFileNm=arguments[4]

    finalfile=os.path.join(trgtFilePath,trgtFileNm)
    spark=SparkSession.builder.appName("appName").getOrCreate()
    sc=spark.sparkContext


    rdd=sc.textFile(lookup)
    header=rdd.first()
    rddforbroadcast=rdd.filter(lambda x: x!=header).map(lambda x: ((x.split(",")[0],x.split(",")[1]),(x.split(",")[2],x.split(",")[3])))
    broadcastVar=sc.broadcast(rddforbroadcast.collectAsMap())
    #print(rdd1.take(5))
    abc=broadcastVar.value.get(('Chico','CA'))
    print(abc[0])
    def updateAMT(city,state,amt):
        ct=city
        st=state
        print(repr(ct))
        print(repr(st))
        v=(ct,st)
        broadcastresult=broadcastVar.value.get(v)
        if broadcastresult is None:
            result=amt
        else:
            result=int(amt)+((int(amt)*int(broadcastresult[0]))/100)
        return result
    spark.udf.register("updateAMT",updateAMT)

    def updateDate(city,state,amtDate):
        ct=city
        st=state
        broadcastresult=broadcastVar.value.get((ct,st))
        if broadcastresult is None:
            result=amtDate
        else:
            result=broadcastresult[1]
        return result
    spark.udf.register("updateDate",updateDate)

    df=spark.read.csv(srcFile, header=True)
    df.createOrReplaceTempView("myview")
    newdf=spark.sql('''select `OrgName`,`Cities`,`ST`,updateAMT(`Cities`,`ST`,`AnyAMT`) AnyAMT, updateDate(`Cities`,`ST`,`AnyAMTDate`) AnyDATE from myview''') 
    #print(newdf.show(2))
    #df_amt=df.withColumn('updated_amt',updateAMT(sf.lit(df.Cities),sf.lit(df.ST),sf.lit(df.AnyAMT)))#.withColumn('updated_date',updateDate(sf.lit('Cities'),sf.lit('ST'),sf.lit('AnyAMTDate')))
    #print(df_amt.show(2))
    newdf.coalesce(1).write.mode("Overwrite").format("csv").save(finalfile)
    sc.stop()


if __name__=='__main__':
    main()

    