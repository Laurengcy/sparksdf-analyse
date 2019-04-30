'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-04-30 11:29:04
@LastEditTime: 2019-04-30 11:36:32
'''

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import *

@F.pandas_udf(returnType=ArrayType(StringType()))
def convertToArray(series, delimiter, rm_whitespace):
    if rm_whitespace:
        return series.map(lambda strng: strng.replace(' ', '').split(delimiter))
    else:
        return series.map(lambda strng: strng.split(delimiter))

def ColtoArray(sparks_DF, colname, delimiter=',', rm_whitespace=True):
    sparks_DF = sparks_DF.withColumn(colname, convertToArray(postalData.select(colname), delimiter, rm_whitespace))

    
    

