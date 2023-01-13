from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import rtrim, ltrim
from pyspark.sql.functions import col, array, when, array_remove, lit, size, array_contains




def replace_empty_space(input_df):
    input_df = input_df.replace('', None)
    return input_df


def trim(input_df):
    for c_name in input_df.columns:
        input_df = input_df.withColumn(c_name, ltrim(rtrim(col(c_name))))
    return input_df



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

    # rename primary key column in actual df to avoid ambiguity
    actual_df_D = actual_df.withColumn(primary_key_column + 'D', lit(col(primary_key_column)))
    actual_df_D = actual_df_D.drop(col(primary_key_column))
    # actual_df.printSchema()

    # compare records column wise and adding non matching column names into array
    conditions = [when(expected_df[c] != actual_df_D[c], lit(c)).otherwise("") for c in expected_df.columns if
                  c != primary_key_column]

    select_exp = [col(primary_key_column), array_remove(array(*conditions), "").alias("column_names")]

    mismatch_columns_df = expected_df.join(actual_df_D,
                                           col(primary_key_column + 'D') == col(primary_key_column)).select(select_exp)
    # mismatch_columns_df.show(truncate=False)
    print('Count of all records:', mismatch_columns_df.count())
    print('Count of records having mismatch:', mismatch_columns_df.filter(size("column_names") > 0).count())
    print('Count of records matching:', mismatch_columns_df.filter(size("column_names") == 0).count())
    # print the records having mismatch
    mismatch_columns_df.filter(size("column_names") > 0).show(truncate=False)

    # records having mismatch for perticular column
    mismatch_columns_df.filter(array_contains(col('column_names'), "pvvversionno")).filter(
        size("column_names") > 0).show(truncate=False)

    mismatch_columns_df.select('column_names').distinct().show(truncate=False)

    mismatch_columns_df.createOrReplaceTempView("mcols")

    actual_df_D.createOrReplaceTempView("actual")
    expected_df.createOrReplaceTempView("expected")

