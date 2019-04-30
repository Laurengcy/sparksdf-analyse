'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@LastEditors: laurengcy
@Date: 2019-04-29 11:30:44
@LastEditTime: 2019-04-30 11:31:21
'''

import pyspark

def display(sparks_DF, n=1):
    print ('################ SCHEMA ################')
    sparks_DF.printSchema()
    print ('################ TAKE ################')
    # print (sparks_DF.take(n))
    # TODO: doesnt work
    print ('take doesnt work for now')
    print ('################ SHOW ################')
    sparks_DF.show(n)
    return sparks_DF.SCHEMA

     