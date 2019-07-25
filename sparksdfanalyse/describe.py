'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-04-29 11:30:44
@LastEditTime: 2019-07-01 11:43:08
'''

import pyspark
import pyspark.sql.types as type
import pyspark.sql.functions as F
from sparksdfanalyse.convert import ColToList
from scipy import stats
import numpy as np
import math


def get_shape(DF):
    n_rows = DF.count()
    n_columns = len(DF.columns)
    print ('Data Shape: (%d rows, %d columns)' % (n_rows,n_columns))
    return n_rows, n_columns

def show_schema_n_head(DF, n=1):
    print ('################ SCHEMA ################')
    DF.printSchema()
    print ('################ SHOW ################')
    DF.show(n)
    return DF.SCHEMA
 

def show_n_save(DF, filename):
    DF.write.mode('overwrite').option("header","true").csv(filename + '.csv')
    DF.show()
    return None


def get_distinct_col_values(DF, exclude_col=[], export=True, DF_name=''):
    if export:
        DF = None
    wanted_cols =[]
    for col_name in DF.columns:
        if col_name not in exclude_col:
            if export:
                wanted_cols.append(col_name)
                if DF==None:
                    DF = DF.select(col_name).distinct().orderBy(col_name, ascending=True)
                    # add id col to merge columns
                    DF = DF.withColumn("id", F.monotonically_increasing_id())
                else:
                    # add id col to merge columns
                    add_this = DF.select(col_name).distinct().withColumn("id", monotonically_increasing_id())
                    DF = DF.join(add_this,on='id' ,how="full")
            else:
                DF.select(col_name).distinct().show()
    if export:
        # drop id columns
        DF = DF.select([c for c in DF.columns if c in wanted_cols])
        DF.toPandas().to_csv(DF_name + ' distinct_values_of_each_col.csv')
    return DF

def get_stats(DF, colname):
    stats_as_rows_list = DF.select(colname).describe().collect()
    stats = {}
    for stat_name, value in stats_as_rows_list:
        stats[stat_name] = float(value)
    # count, mean, stddev, min, max
    return stats

def get_outliers_and_filtered_data(DF, colname, z_threshold=3.0):
    stats = get_stats(DF, colname)
    outlier_df = DF.filter(F.abs(F.col(colname)-F.lit(stats['mean'])) > F.lit(z_threshold*stats['stddev']))
    filtered_df = DF.filter(F.abs(F.col(colname)-F.lit(stats['mean'])) <= F.lit(z_threshold*stats['stddev']))
    return outlier_df, filtered_df
    # data = np.array(ColToList(DF, colname))
    # z_scores = np.abs(stats.zscore(data))
    # return data[z_scores>=z_threshold], data[z_scores<z_threshold]
