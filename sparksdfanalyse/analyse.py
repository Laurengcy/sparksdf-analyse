'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-04-24 12:21:09
@LastEditTime: 2019-04-24 12:27:58
'''

""" 
get_shape(sparks_DF)
get_distinct_col_values(sparks_DF, exclude_col=[], export=True, DF_name='')
get_col_stats(sparks_DF, exclude_col=[])
"""

import pyspark
import pyspark.sql.types as types
from pyspark.sql.functions import monotonically_increasing_id
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt

def get_shape(sparks_DF):
    n_rows = sparks_DF.count()
    n_columns = len(sparks_DF.columns)
    print ('Data Shape: (%d rows, %d columns)' % (n_rows,n_columns))
    print (sparks_DF.schema.fields)
    return n_rows, n_columns

def get_distinct_col_values(sparks_DF, exclude_col=[], export=True, DF_name=''):
    if export:
        DF = None
    wanted_cols =[]
    for col_name in sparks_DF.columns:
        if col_name not in exclude_col:
            if export:
                wanted_cols.append(col_name)
                if DF==None:
                    DF = sparks_DF.select(col_name).distinct().orderBy(col_name, ascending=True)
                    # add id col to merge columns
                    DF = DF.withColumn("id", monotonically_increasing_id())
                else:
                    # add id col to merge columns
                    add_this = sparks_DF.select(col_name).distinct().withColumn("id", monotonically_increasing_id())
                    DF = DF.join(add_this,on='id' ,how="full")
            else:
                sparks_DF.select(col_name).distinct().show()
    if export:
        # drop id columns
        DF = DF.select([c for c in DF.columns if c in wanted_cols])
        DF.toPandas().to_csv(DF_name + ' distinct_values_of_each_col.csv')
    return None


# for numeric columns only
def get_col_stats(sparks_DF, exclude_col=[]):
    fig, ax = plt.subplots()

    for struct_field in sparks_DF.schema.fields:
        if struct_field.name not in exclude_col:
            if isinstance(struct_field.dataType, types.NumericType):
                sparks_DF.describe(struct_field.name).show()
                hist(ax, sparks_DF.select(struct_field.name), bins = 3, color=['red'])
                plt.show()
    return None
    