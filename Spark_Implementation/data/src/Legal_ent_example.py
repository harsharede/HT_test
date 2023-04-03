from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import max, sum, col, lit
from functools import reduce

# create SparkSession
spark = SparkSession.builder.appName('myApp').getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# load datasets
df1 = spark.read.csv('/opt/mnt/input/legal_entity/dataset1.csv', header=True)
df2 = spark.read.csv('/opt/mnt/input/legal_entity/dataset2.csv', header=True)

# merge datasets
merged_df = df1.join(df2, on='counter_party')

# calculate max rating by counterparty
max_rating = merged_df.groupBy('legal_entity', 'counter_party', 'tier').agg(max('rating').alias('max_rating'))

# calculate sum of value where status = ARAP
arap_sum = merged_df.filter(col('status') == 'ARAP').groupBy('legal_entity', 'counter_party', 'tier').agg(sum('value').alias('arap_sum'))

# calculate sum of value where status = ACCR
accr_sum = merged_df.filter(col('status') == 'ACCR').groupBy('legal_entity', 'counter_party', 'tier').agg(sum('value').alias('accr_sum'))

# merge the above dataframes to get the final result
result_df = max_rating.join(arap_sum, on=['legal_entity', 'counter_party', 'tier'], how='outer').join(accr_sum, on=['legal_entity', 'counter_party', 'tier'], how='outer').fillna(0)

# calculate total for each legal_entity, counter_party, and tier
total_df = result_df.groupBy('legal_entity', 'counter_party', 'tier').agg(max('max_rating').alias('max_rating'), sum('arap_sum').alias('arap_sum'), sum('accr_sum').alias('accr_sum'))

# calculate total for each legal_entity
tota_cal_df_list = [total_df]
col_list = ['legal_entity', 'counter_party', 'tier']
for each in col_list:
    tota_cal_df = total_df.groupBy(each).agg(max('max_rating').alias('max_rating'), sum('arap_sum').alias('arap_sum'), sum('accr_sum').alias('accr_sum'))
    for col_name in col_list:
        if col_name != each:
            tota_cal_df = tota_cal_df.withColumn(col_name, lit('Total'))
    tota_cal_df_list.append(tota_cal_df)


# calculate grand total
grand_total_df = total_df.agg(max('max_rating').alias('max_rating'), sum('arap_sum').alias('arap_sum'), sum('accr_sum').alias('accr_sum'))
grand_total_df = grand_total_df.withColumn('legal_entity', lit('Total')).withColumn('counter_party', lit('Total')).withColumn('tier', lit('Total'))
tota_cal_df_list.append(grand_total_df)

# combine all the dataframes
# final_df = reduce(DataFrame.unionAll, tota_cal_df_list)

final_df = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), tota_cal_df_list)

# show final result
final_df.show(100, False)
