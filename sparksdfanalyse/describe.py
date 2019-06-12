'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-04-29 11:30:44
@LastEditTime: 2019-06-03 15:18:39
'''

import pyspark
import pyspark.sql.types as type
from pyspark.sql.functions import monotonically_increasing_id

def get_shape(sparks_DF):
    n_rows = sparks_DF.count()
    n_columns = len(sparks_DF.columns)
    print ('Data Shape: (%d rows, %d columns)' % (n_rows,n_columns))
    return n_rows, n_columns

def show_schema_n_head(sparks_DF, n=1):
    print ('################ SCHEMA ################')
    sparks_DF.printSchema()
    print ('################ SHOW ################')
    sparks_DF.show(n)
    return sparks_DF.SCHEMA
 

def show_n_save(sparks_DF, filename, partition=1):
    sparks_DF
    sparks_DF.show()
    return None


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
    return DF


# for numeric columns only
# def get_col_stats(sparks_DF, exclude_col=[]):
#     fig, ax = plt.subplots()

#     for struct_field in sparks_DF.schema.fields:
#         if struct_field.name not in exclude_col:
#             if isinstance(struct_field.dataType, types.NumericType):
#                 sparks_DF.describe(struct_field.name).show()
#                 hist(ax, sparks_DF.select(struct_field.name), bins = 3, color=['red'])
#                 plt.show()
#     return None
    