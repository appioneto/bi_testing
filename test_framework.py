#imports
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import builtins as p
import pandas as pd

# Functions made by Marlon Basani

#function to get schema of df
def checkSchema(df):
  """To check documentation between databricks table and confluence page.

  Parameters:
  df (dataFrame): DataFrame with data

  Returns:
  df: Data Frame columns:
  field_name: used to compair the columns name in confluence page
  data_type: used dto compair the data type in confluence page
  qty_nul: quantity of rows with null values per column. 
  """
    
  collect_df = []
  for i in df.columns:
    collect_df.append({"column_name": i 
                       ,"data_type": df.select(i).dtypes[0][1] 
                       ,"qty_null": df.select(count(when(col(i) == "", i).when(col(i).isNull(), i)).alias(i)).collect()[0][0]}
                     )
  df_result = spark.createDataFrame(collect_df)
  return df_result.select('column_name', 'data_type', 'qty_null')


def checkDistinct(df):
  """To check the percentual of distincts registers in all dataframe columns.

    Parameters:
    df (dataFrame): DataFrame with data

    Returns:
    df: Data Frame columns:
      field_name: Column name
      pct_unique_count: (total of distinct values in column / total number of rows) * 100
 """
  collect_df = []
  qty_rows = df.count()
  for i in df.columns:
    collect_df.append({"field_name": i
                       ,"pct_unique_count": (df.select(col(i)).distinct().count()/qty_rows) * 100}
                     )
  df_result = spark.createDataFrame(collect_df)
  return df_result.select('field_name', 'pct_unique_count')


def checkColumsDistinct(df,lst_columns):
  """To check the percentual of distincts registers of specifcs columns.

  Parameters
  ----------
  df : datafrane      
    DataFrame with data to check
  lst_columns: list
    list of columns should be unique


  Returns
  ----------
   float: percentual of distincts values
  """
  qty_rows = df.count()
  qty_rows_distinct = df.select(lst_columns).distinct().count()
  return (qty_rows_distinct/qty_rows)*100


def checkActiveRows(df,lst_columns,column_filter = 'flg_scd_valid_now',value_filter = '1'):
  """To check the percentual of distincts active registers

  Parameters
  ----------
  df : datafrane      
    DataFrame with data to check
  lst_columns: list
    list of columns should be unique
  column_filter: str, optional
    column used to filter active rows (default is 'flg_scd_valid_now')
  value_filter: str
    value used to identfy active rows (default is '1')
  
  Returns
  ----------
   float: percentual of distincts values
  """
  
  df_filtered = __filter_df(df, column_filter, value_filter)
  qty_rows = df_filtered.count()
  qty_rows_distinct = df_filtered.select(lst_columns).where(col(column_filter) ==value_filter).distinct().count()
  return (qty_rows_distinct/qty_rows)*100


def checkDtmValidTo (df, end_date_column="dtm_scd_valid_to", column_filter = 'flg_scd_valid_now', value_filter = '1'):
  df_filtered = __filter_df(df, column_filter, value_filter)
  
  df_result = df_filtered.select(end_date_column).distinct()
  if (df_result.count()== 1) & (df_result.collect()[0][0].date() == datetime.strptime('9999-12-31', '%Y-%m-%d').date()):
    return('APPROVED')
  else:
    return('NOT APPROVED')
  

def checkInsertSCD2(df, lst_partitionby, start_date_column="dtm_scd_valid_from", end_date_column="dtm_scd_valid_to"):
  """To check the start date column and end date column in rows scd2 

  Parameters
  ----------
  df : datafrane      
    dataFrame with data to check
  lst_partitionby: list
    name of buk column
  start_date_column: str, optional
    name of column that contains the start date of the row (default is 'dtm_scd_valid_from')
 end_date_column: str, optional
    name of column that contains the end date of the row (default is 'dtm_scd_valid_to')
  Returns
  ----------
  df: dataFrame with rows that can be a divergence
  """
  
  windowSpec = Window.partitionBy([col(c) for c in lst_partitionby]).orderBy(start_date_column)
  df_date = df
  #create columns with previous register
  df = df.withColumn('lag_end_date' , lag(end_date_column, 1).over(windowSpec)).withColumn('lead_start_date', lead(start_date_column, 1).over(windowSpec))
  df = df.withColumn('calc_start_date', when(df.lag_end_date.isNotNull(), date_add(col('lag_end_date'),1)).otherwise(date_format(col(start_date_column),'yyyy-MM-dd'))).withColumn('calc_end_date',  when(df.lead_start_date.isNotNull(), date_add(col('lead_start_date'),-1)).otherwise('9999-12-31').alias('calc_end_date'))
  df_check_date = df.where(col(start_date_column) != col('calc_start_date'))
  
  return df_check_date
  

# Functions made by Appio Neto

def checkAllowedValuesColumn(df, columnToValidate, allowed_values):
    """ To check the percentual of distincts registers of specifcs columns.

  Parameters
  ----------
  df : dataframe
    DataFrame with data to check
  columnToValidate : string
    column to check allowed values
  allowed_values : list
    list of distinct values to validate
  
  Returns
  ----------
   List: List with not allowed values
  """
    wrong_values = df.select(col(columnToValidate)).distinct().filter(~col(columnToValidate).isin(allowed_values)).rdd.flatMap(lambda x: x).collect()
    if len(wrong_values) > 0:
        message = "Test Failed: Not allowed values list: " + str(wrong_values)
    else: message = "Test Passed: There are only allowed values"
    return print(message)


def topTenValuesDetailed(df):
    """ To show a sample of 10 distinct values from each column of dataframe.

  Parameters
  ----------
  df : dataframe
    DataFrame with data to check

  Returns
  ----------
  String: A string from each column with 10 distinct values
"""  
    labelLen = []
    maxLen = 0

    for i in df.columns:
        labelLen.append(len(i))
        maxLen = p.max(labelLen)
    message = []
    for i in df.columns:
        lpadName = i
        columnType = dict(df.dtypes)[i]
        entity_max_len = df.withColumn("len_", length(col(i))).select(col(i), col('len_')).sort(col('len_').desc(), col(i).desc()).limit(1).collect()[0][0]
        max_len = df.withColumn("len_", length(col(i))).select(col(i), col('len_')).sort(col('len_').desc()).limit(1).collect()[0][1]
        entity_min_len = df.withColumn("len_", length(col(i))).select(col(i), col('len_')).sort(col('len_').asc(), col(i).asc()).limit(1).collect()[0][0]
        min_len = df.withColumn("len_", length(col(i))).select(col(i), col('len_')).sort(col('len_').asc()).limit(1).collect()[0][1]
        message1 =  ":"
        message2 = " : ".join(map(str, df.filter(length(col(i)) < 50).select(col(i)).distinct().rdd.flatMap(lambda x: x).collect()[0:10]))
        print("Column name: " + lpadName, "\n"
              "Data type: "+columnType,"\n"
              "Largest value with length " + str(max_len) +": " + str(entity_max_len),"\n"
              "Smaller value with length " + str(min_len) +": " + str(entity_min_len),"\n"
              "Data sample: ", message2, "\n")



def countingTables (df1, df2):
    """ To compare whether 2 dataframes had same count of rows

  Parameters
  ----------
  df1 : first dataframe
  df2 : second dataframe
  
  Returns
  ----------
   String: if quantity rows are equal in both dataframes the message says "approved", else "not approved"
  """    
    count_df1 = df1.count()
    count_df2 = df2.count()
    if count_df1 == count_df2:
        return('APPROVED')
    else:
        return('NOT APPROVED')


def checkLimitsOfColumn(df, columnName, minValue, maxValue):
    """ To check min and max values of specifcs columns.

  Parameters
  ----------
  df : dataframe
    DataFrame with data to check
  columnName : string
    column to check min and max values
  minValue : number
    min value allowed for specific column
  maxValue : number
    max value allowed for specific column    
  
  Returns
  ----------
   List: List with not allowed values
  """
    checkMin = df.select(min(col(columnName))).collect()[0][0]
    checkMax = df.select(max(col(columnName))).collect()[0][0]
    if (checkMin >= minValue) & (checkMax <= maxValue):
        message = "Test Passed"
    else: message = "Test Failed: Minimum an Maximum Value are: " + str(checkMin) + " and " + str(checkMax)
    return print(message)






#Test if a table exists
def tableExists(schema, table_name):
  return spark.catalog.tableExists(f"{schema}.{table_name}")

