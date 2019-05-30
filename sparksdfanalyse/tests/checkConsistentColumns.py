'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@Date: 2019-05-30 15:34:45
@LastEditors: laurengcy
@LastEditTime: 2019-05-30 16:25:05
'''


import sparksdfanalyse
from sparksdfanalyse.check import checkConsistentColumns
import os

mod_dir= os.path.dirname(sparksdfanalyse.__file__)
test_data_dir = os.path.join(mod_dir, 'tests', 'data')

if __name__ == "__main__":
    checkConsistentColumns(test_data_dir)