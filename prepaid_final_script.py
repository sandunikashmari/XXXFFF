
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


spark = SparkSession.builder.master("yarn").appName("Daily_prepaid_usage_behaviors").enableHiveSupport().getOrCreate()


SGV_daily_data_df = spark.sql("SELECT * FROM report_data.prepaid_usage_behaviors") \
    .withColumn("mobile_no", F.lower(F.trim(F.col("mobile_no")))) \
    .withColumn("debit_led_acc_code", F.lower(F.trim(F.col("debit_led_acc_code"))))

customer360_df = spark.sql(""" 
    SELECT lower(trim(mobile_no)) AS mobile_no, 
           district,
           last_vlr_date,
           prefered_language,
           rpu_90_days,
           debit_led_acc_code
   FROM customer360.pre_customer360_summary 
""")

device_type = spark.sql(""" 
    SELECT lower(trim(mobile_no)) AS mobile_no, device_type 
    FROM report_data.pos_device_type_active
""")

recharge_daily_df = spark.sql(""" 
    SELECT
        LOWER(TRIM(mobile_no)) AS mobile_no,
        debit_led_acc_code,
        SUM(recharge_amt) AS total_recharge,
        SUM(CASE WHEN dateval >= DATE_SUB(CURRENT_DATE(), 90) THEN recharge_amt ELSE 0 END) AS total_recharge_last_3_months,
        SUM(CASE WHEN dateval >= DATE_SUB(CURRENT_DATE(), 30) THEN recharge_amt ELSE 0 END) AS total_recharge_last_30_days
    FROM customer360.pre_recharge_daily
    WHERE dateval <= CURRENT_DATE()
      AND dateval >= DATE_SUB(CURRENT_DATE(), 365)
    GROUP BY mobile_no, debit_led_acc_code
    ORDER BY mobile_no
""")

mon_vas_currentplan = spark.sql(""" 
    SELECT  LOWER(TRIM(mobile_no)) AS mobile_no, debit_led_acc_code, dateval AS current_plan_date, product_id AS current_plan, deducted_amt
    FROM (
        SELECT mobile_no, debit_led_acc_code, dateval, product_id, deducted_amt,
               ROW_NUMBER() OVER (PARTITION BY mobile_no ORDER BY dateval DESC) AS rn
        FROM customer360.prepaid_mon_vas_package_activations
        WHERE deducted_amt > 0
    ) tmp
    WHERE rn = 1
""")

mon_vas_favplan = spark.sql(""" 
    SELECT 
        LOWER(TRIM(mobile_no)) AS mobile_no, 
        debit_led_acc_code, 
        favourite_plan,
        product_activation_count,
        DATE_SUB(CURRENT_DATE(), 90) AS start_date_fav_plan, 
        CURRENT_DATE() AS end_date_fav_plan 
    FROM (
        SELECT
            LOWER(TRIM(mobile_no)) AS mobile_no,
            debit_led_acc_code,
            product_id AS favourite_plan,
            COUNT(product_id) AS product_activation_count,
            ROW_NUMBER() OVER (
                PARTITION BY mobile_no
                ORDER BY COUNT(product_id) DESC
            ) AS rank
        FROM
            customer360.prepaid_mon_vas_package_activations
        WHERE
            dateval >= DATE_SUB(CURRENT_DATE(), 90) 
            AND deducted_amt > 0
        GROUP BY
            mobile_no,
            debit_led_acc_code,
            product_id
    ) ranked_activations
    WHERE
        rank = 1
""")

mon_vas_final_df = mon_vas_currentplan.join(
    mon_vas_favplan, 
    on=["mobile_no", "debit_led_acc_code"], 
    how="outer"
)

SGV_customer360_df = SGV_daily_data_df.join(
    customer360_df,
    on=["mobile_no", "debit_led_acc_code"],
    how="left"
)

device_type_360 = SGV_customer360_df.join(
    device_type,
    on=["mobile_no"],
    how="left"
)

SGV_customer360_recharge_df = SGV_customer360_df.join(
    recharge_daily_df,
    on=["mobile_no", "debit_led_acc_code"],
    how="left" 
)


final_df = SGV_customer360_recharge_df.join(
    mon_vas_final_df,
    on=["mobile_no", "debit_led_acc_code"],
    how="left"
)


final_df.write.partitionBy('dateval').mode('overwrite').saveAsTable("report_data.Daily_prepaid_usage_behaviors", format='parquet', path="/data/intern_projects/Daily_prepaid_usage_behaviors")


