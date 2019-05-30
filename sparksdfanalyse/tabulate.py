'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-05-02 14:10:54
@LastEditTime: 2019-05-21 16:19:06
'''

import pyspark
import pyspark.sql.functions as F
from prettytable import PrettyTable
from pyspark.sql.window import Window


def countNullRows(sparks_DF, colnames=None, examples=True, examples_limit=3):
    counts = []
    examples = {}
    if colnames is None:
        colnames = sparks_DF.columns
        assert isinstance(colnames, list)
    for colname in colnames:
        count = sparks_DF.filter((sparks_DF[colname] == '') | sparks_DF[colname].isNull()).count()
        counts.append([colname, count])
        if count >0:
            examples[colname]= sparks_DF.filter((sparks_DF[colname] == '') | sparks_DF[colname].isNull()).limit(examples_limit)
    counts_T = PrettyTable(['Colname', 'NullCounts'])
    for l in counts:
        counts_T.add_row(l)
    print (counts_T)
    return [counts, examples]
    

def get_percentage(sparks_DF, numeral_colname, round_to=2, colname='percent'):
    sparks_DF = sparks_DF.withColumn(colname, 100*F.col(numeral_colname)/F.sum(numeral_colname).over(Window.partitionBy()))
    sparks_DF = sparks_DF.withColumn(colname, F.round(F.col(colname), round_to))
    sparks_DF = sparks_DF.orderBy(F.asc(colname))
    return sparks_DF
    
 