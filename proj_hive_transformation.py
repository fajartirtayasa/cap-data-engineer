from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def read_transform_load():
    '''
    Fungsi ini digunakan untuk membaca tabel yang ada pada Hive. Kemudian fungsi ini juga
    dapat melakukan beberapa transformasi pada Spark DataFrame, seperti mengubah format tanggal,
    melakukan filtering data, mengganti nama kolom, menambah kolom baru, serta melakukan drop kolom.

    Return:
        df -> spark dataframe
    '''
    #buat SparkSeddion
    spark = SparkSession.builder\
        .appName('NYC Green Taxi')\
        .config('spark.sql.catalogImplementation', 'hive')\
        .config('hive.metastore.uris', 'thrift://localhost:9083')\
        .enableHiveSupport()\
        .getOrCreate()

    #tentukan database Hive
    spark.sql('use green_taxi_staging')

    #tentukan tabel Hive
    df = spark.table('green_taxi_2020')

    #mengubah data kolom `lpep_pickup_datetime` dan `lpep_dropoff_datetime` dalam format 'YYYY-MM'DD'
    df = df.withColumn('lpep_pickup_datetime', df['lpep_pickup_datetime']/1000000)\
        .withColumn('lpep_dropoff_datetime', df['lpep_dropoff_datetime']/1000000)
    df = df.withColumn('lpep_pickup_datetime',
                       from_unixtime(df['lpep_pickup_datetime'], 'yyyy-MM-dd HH:mm:ss'))\
        .withColumn('lpep_dropoff_datetime',
                    from_unixtime(df['lpep_dropoff_datetime'], 'yyyy-MM-dd HH:mm:ss'))
    
    #melakukan filtering untuk data tahun 2020 saja
    df = df.filter((df['lpep_pickup_datetime'] >= '2020-01-01') & (df['lpep_pickup_datetime'] <= '2020-12-31'))

    #mengubah beberapa nama kolom
    df = df.withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime')\
        .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
    
    #membuat kolom berisi durasi perjalanan
    df = df.withColumn('trip_duration_s', expr('unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)'))
    
    #drop kolom 'dropoff_datetime' dan 'ehail_fee'
    df = df.drop(*['dropoff_datetime', 'ehail_fee'])
    
    

    #buat kolom untuk dimensi `vendor`
    df = df.withColumn('vendor_name', when(df['VendorID'] == 1, 'Creative Mobile Technologies, LLC')\
                        .when(df['VendorID'] == 2, 'VeriFone Inc'))
    df = df.withColumn('SK_vendor', when(df['vendor_name'] == 'Creative Mobile Technologies, LLC', 1)\
                        .when(df['vendor_name'] == 'VeriFone Inc' , 2))

    #buat kolom untuk dimensi 'rate_code'
    df = df.withColumn('rate_detail', when(df['RateCodeID'] == 1, 'Standard rate')\
                        .when(df['RateCodeID'] == 2, 'JFK')\
                        .when(df['RateCodeID'] == 3, 'Newark')\
                        .when(df['RateCodeID'] == 4, 'Nassau or Westchester')\
                        .when(df['RateCodeID'] == 5, 'Negotiated fare')\
                        .when(df['RateCodeID'] == 6, 'Group ride'))
    df = df.withColumn('SK_rate', when(df['rate_detail'] == 'Standard rate', 1)\
                        .when(df['rate_detail'] == 'JFK' , 2)\
                        .when(df['rate_detail'] == 'Newark', 3)\
                        .when(df['rate_detail'] == 'Nassau or Westchester', 4)\
                        .when(df['rate_detail'] == 'Negotiated fare', 5)\
                        .when(df['rate_detail'] == 'Group ride', 6))

    #buat kolom untuk dimensi 'store_and_fwd'
    df = df.withColumn('store_fwd_description', when(df['Store_and_fwd_flag'] == 'Y', 'store and forward trip')\
                        .when(df['Store_and_fwd_flag'] == 'N', 'not a store and forward trip'))
    df = df.withColumn('store_fwd_id', when(df['store_fwd_description'] == 'store and forward trip', 1)\
                        .when(df['store_fwd_description'] == 'not a store and forward trip', 2))

    #buat kolom untuk dimensi 'payment_type'
    df = df.withColumn('payment_name', when(df['Payment_type'] == 1, 'Credit card')\
                        .when(df['Payment_type'] == 2, 'Cash')\
                        .when(df['Payment_type'] == 3, 'No charge') 
                        .when(df['Payment_type'] == 4, 'Dispute')\
                        .when(df['Payment_type'] == 5, 'Unknown')\
                        .when(df['Payment_type'] == 6, 'Voided trip'))
    df = df.withColumn('SK_payment', when(df['payment_name'] == 'Credit card', 1)\
                        .when(df['payment_name'] == 'Cash', 2)\
                        .when(df['payment_name'] == 'No charge', 3)\
                        .when(df['payment_name'] == 'Dispute', 4)\
                        .when(df['payment_name'] == 'Unknown', 5)\
                        .when(df['payment_name'] == 'Voided trip', 6))
    
    #buat kolom untuk dimensi 'trip_type'
    df = df.withColumn('trip_description', when(df['Trip_type'] == 1, 'Street-hail')\
                        .when(df['Trip_type'] == 2, 'Dispatch'))
    df = df.withColumn('SK_trip', when(df['trip_description'] == 'Street-hail', 1)\
                        .when(df['trip_description'] == 'Dispatch', 2))
    
    #tambahkan kolom untuk tanggal saat ini
    df = df.withColumn('StartDate', current_date())

    #tambahkan kolom untuk tanggal '9999-12-01'
    df = df.withColumn('EndDate', lit('9999-12-31'))

    #tambahkan kolom untuk id di fact table
    df = df.withColumn('history_id', monotonically_increasing_id() + 1)

    

    #buat dataframe untuk 'fact_table'
    fact_table = df.selectExpr('history_id', 'pickup_datetime', 'VendorID', 'RateCodeID', 'store_fwd_id', 'Payment_type', 'Trip_type',
                               'PULocationID', 'DOLocationID', 'passenger_count', 'trip_duration_s', 'trip_distance', 'fare_amount',
                               'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'Improvement_surcharge', 'congestion_surcharge',
                               'total_amount')
    
    #buat dataframe untuk tabel-tabel dimensi
    dim_vendor = df.selectExpr('SK_vendor', 'VendorID', 'vendor_name', 'StartDate', 'EndDate').distinct()\
        .filter(df['vendor_name'].isNotNull()).orderBy('VendorID')
    dim_ratecode = df.selectExpr('SK_rate', 'RateCodeID', 'rate_detail', 'StartDate', 'EndDate').distinct()\
        .filter(df['rate_detail'].isNotNull()).orderBy('SK_rate')
    dim_store_fwd = df.selectExpr('store_fwd_id', 'Store_and_fwd_flag', 'store_fwd_description', 'StartDate', 'EndDate').distinct()\
        .filter(df['store_fwd_description'].isNotNull()).orderBy('store_fwd_id')
    dim_payment = df.selectExpr('SK_payment', 'Payment_type', 'payment_name', 'StartDate', 'EndDate').distinct()\
        .filter(df['payment_name'].isNotNull()).orderBy('Payment_type')
    dim_trip = df.selectExpr('SK_trip', 'Trip_type', 'trip_description', 'StartDate', 'EndDate').distinct()\
        .filter(df['trip_description'].isNotNull()).orderBy('Trip_type')
    
    # fact_table.show(5)
    # dim_vendor.show()
    # dim_ratecode.show()
    # dim_store_fwd.show()
    # dim_payment.show()
    # dim_trip.show()


    #load fact dan dimension table ke hive
    spark.sql('CREATE DATABASE IF NOT EXISTS green_taxi')
    spark.sql('USE green_taxi')

    fact_table.write.mode('overwrite').saveAsTable('fact_table')
    # dim_vendor.write.mode('overwrite').saveAsTable('dim_vendor')
    dim_ratecode.write.mode('overwrite').saveAsTable('dim_ratecode')
    dim_store_fwd.write.mode('overwrite').saveAsTable('dim_store_fwd')
    dim_payment.write.mode('overwrite').saveAsTable('dim_payment')
    dim_trip.write.mode('overwrite').saveAsTable('dim_trip')