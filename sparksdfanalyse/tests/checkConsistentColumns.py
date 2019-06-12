'''
@Description: 
@Author: laurengcy
@Github: https://github.com/laurengcy
@Date: 2019-05-30 15:34:45
@LastEditors: laurengcy
@LastEditTime: 2019-05-30 17:30:22
'''


import sparksdfanalyse
from sparksdfanalyse.check import checkConsistentColumns
import os

mod_dir= os.path.dirname(sparksdfanalyse.__file__)
test_data_dir = os.path.join(mod_dir, 'tests', 'data')

# test_data_dir='/Users/lauren_goh/OneDrive - Singapore University of Technology and Design/SIA_DS/0_Krispay/Data/raw/KrisPay_transactions'

if __name__ == "__main__":
    checkConsistentColumns(test_data_dir)