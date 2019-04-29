'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-04-29 12:02:44
@LastEditTime: 2019-04-29 12:16:51
'''


import pyspark


def replace_whitespace_colnames(sparks_DF):
    old_colnames = sparks_DF.columns
    new_colnames = []
    for name in old_colnames:
        new_colnames.append(name.replace(' ', '_'))
    for i in range(len(old_colnames)):
        sparks_DF = sparks_DF.withColumnRenamed(old_colnames[i], new_colnames[i])
    return sparks_DF