'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-04-29 11:30:44
@LastEditTime: 2019-06-12 17:07:46
'''

import pyspark
import pyspark.sql.types as type
from pyspark.sql.functions import monotonically_increasing_id
from sparksdfanalyse.convert import ColToList
from scipy import stats


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
 

def show_n_save(DF, filename, partition=1):
    DF
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
                    DF = DF.withColumn("id", monotonically_increasing_id())
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

def get_outliers_and_filtered_data(DF, colname, z_threshold=3.0):
    data = np.array(ColToList(DF, colname))
    z_scores = np.abs(stats.zscore(data))
    return data[z_scores>=z_threshold], data[z_scores<z_threshold]
