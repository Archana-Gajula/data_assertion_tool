from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, array, when, array_remove, lit, size, array_contains



if __name__ == '__main__':

    expected_file_path = 'data/expected_data.csv'
    actual_file_path = 'data/actual_data.csv'
    primary_key_column = 'vendor_id'

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    actual_df = spark.read.csv(actual_file_path, sep=',', header=True)
    expected_df = spark.read.csv(expected_file_path, sep=',', header=True)


    print("Count of all records from expected:", expected_df.count())
    print("Distinct Count of records from expected:", expected_df.distinct().count())
    print("Count of all records from actual:", actual_df.count())
    print("Distinct Count of records from actual:", actual_df.distinct().count())

    common_records_df = expected_df.intersect(actual_df)
    print("Count of records that are in both:", common_records_df.count())

    only_expected_df = expected_df.subtract(actual_df)

    only_actual_df = actual_df.subtract(expected_df)

    print("Some non common records from expected:")
    expected_non_matching_records = only_expected_df.join(only_actual_df, on=primary_key_column, how='inner').select(
        only_expected_df["*"])
    expected_non_matching_records.orderBy(F.col(primary_key_column).asc()).show(5, False)

    print("Some non common records from actual:")
    actual_non_matching_records = only_actual_df.join(only_expected_df, on=primary_key_column, how='inner').select(
        only_actual_df["*"])
    actual_non_matching_records.orderBy(F.col(primary_key_column).asc()).show(5, False)

    # compare records column wise and adding non matching column names into array
    conditions = [when(expected_df[c] != actual_df[c], lit(c)).otherwise("") for c in expected_df.columns]
#     print("conditions:",conditions)
    select_exp = [col(primary_key_column), array_remove(array(*conditions), "").alias("column_names")]
#     print("select_exp:",select_exp)
    mismatch_columns_df = expected_df.join(actual_df,
                                           on=primary_key_column).select(select_exp)
#     mismatch_columns_df.printSchema()

    print('Count of all records:', mismatch_columns_df.count())
    print('Count of records having mismatch:', mismatch_columns_df.filter(size("column_names") > 0).count())
    print('Count of records matching:', mismatch_columns_df.filter(size("column_names") == 0).count())
    # print the records having mismatch
    mismatch_columns_df.filter(size("column_names") > 0).show(truncate=False)

    # records having mismatch for perticular column
    mismatch_columns_df.filter(array_contains(col('column_names'), "pvvversionno")).filter(
        size("column_names") > 0).show(truncate=False)

    mismatch_columns_df.select('column_names').distinct().show(truncate=False)

