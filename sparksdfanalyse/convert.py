'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-04-30 11:29:04
@LastEditTime: 2019-09-05 15:34:26
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

def ColStringToColArray(DF, colname, delimiter=',', rm_whitespace=True):
    DF = DF.withColumn(colname, StringToArray(DF[colname], delimiter, rm_whitespace))

def ColToList(DF, colname):
    return DF.select(colname).rdd.flatMap(lambda x:x).collect()
    # return F.collect_list(DF.select(colname)).collect()

def RowToList(DF):
    return DF

def get_lowercase_stripped_colnames(DF):
    old_colnames = DF.columns
    
    remove = string.punctuation
    remove = remove.replace("_", "") # don't remove underscores
    pattern = r"[{}]".format(remove) # create the pattern
    
    for colname in DF.columns:
        new_name = colname.strip()
        new_name = re.sub(pattern, "", new_name) 
        new_name = new_name.replace(' ', '_')
        new_name = new_name.lower()
        
        DF = DF.withColumnRenamed(colname, new_name)

    return DF
   
def get_pddf_from_vector_col(DF, colname):
    def extract(row):
        return tuple(row[colname].toArray().tolist())
    
    return DF.rdd.map(extract).toDF([colname]).toPandas()