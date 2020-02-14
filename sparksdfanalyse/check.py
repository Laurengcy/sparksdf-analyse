'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@Date: 2019-05-30 15:28:48
@LastEditors: laurengcy
@LastEditTime: 2019-05-30 17:30:16
'''

import csv
import os
from os.path import basename, normpath
from glob import iglob

def checkConsistentColumns(data_dir):
    consistent = True
    same_cols = []
    columns = None

    while consistent:
        for file in iglob(os.path.join(data_dir, '*.csv')):
            if columns is None:
                columns = set(getColumns(file))
            else:
                if columns == set(getColumns(file)):
                    pass
                else:
                    print (
                        '--'*30 
                        + '\nWARNING: Columns of .csv files are not consistent. \n' 
                        + basename(normpath(file)) + ' has different columns from '
                        + str(same_cols)
                        +'\n'
                        + '--'*30 
                    )
                    consistent = False
                    break
            same_cols.append(basename(normpath(file)))
        break
    return consistent
            

def getColumns(file_dir):
    with open(file_dir, newline='') as content:
        return next(csv.reader(content), [])

def get_duplicates(df, cols_to_check):
    return df.join(
        df.groupBy(cols_to_check).agg((F.count("*")>1).cast("int").alias("duplicated")),
        on=cols_to_check,
        how="inner"
    ).filter(F.col('duplicated')==1)