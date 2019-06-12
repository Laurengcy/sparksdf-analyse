'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-04-30 11:29:04
@LastEditTime: 2019-06-07 10:13:19
'''

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import *

@F.pandas_udf(returnType=ArrayType(StringType()))
def StringToArray(series, delimiter, rm_whitespace):
    if rm_whitespace:
        return series.map(lambda strng: strng.replace(' ', '').split(delimiter))
    else:
        return series.map(lambda strng: strng.split(delimiter))

def ColStringToColArray(sparks_DF, colname, delimiter=',', rm_whitespace=True):
    sparks_DF = sparks_DF.withColumn(colname, StringToArray(sparks_DF[colname], delimiter, rm_whitespace))

def ColToList(sparks_DF, colname):
    return sparks_DF.select(colname).rdd.flatMap(lambda x:x).collect()
    # return F.collect_list(sparks_DF.select(colname)).collect()

def RowToList(sparks_DF):
    return sparks_DF

def replace_whitespace_colnames(sparks_DF):
    old_colnames = sparks_DF.columns
    new_colnames = []
    for name in old_colnames:
        new_colnames.append(name.replace(' ', '_'))
    for i in range(len(old_colnames)):
        sparks_DF = sparks_DF.withColumnRenamed(old_colnames[i], new_colnames[i])
    return sparks_DF
   

