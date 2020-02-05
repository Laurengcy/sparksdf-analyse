'''
E.g. colname_regex_tuples
[('flg_no', '\\d{4}'),
 ('sif', '\\d{6}'),
'''


def print_pyspark_filter_combined_col_regex_function(df_name, colname_regex_tuples, anti=False):
    
    print (f'{df_name}.filter(')
    for idx in range(len(colname_regex_tuples)):
        if idx ==0 and anti:
            print ("~(")
        if idx != len(colname_regex_tuples)-1:
            print ("   (F.col('{}').rlike('{}')) &".format(colname_regex_tuples[idx][0], colname_regex_tuples[idx][1]))
        else:
            print ("   (F.col('{}').rlike('{}'))".format(colname_regex_tuples[idx][0], colname_regex_tuples[idx][1])) 
            if anti:
                print (")")
            print (")")  
            
    return None