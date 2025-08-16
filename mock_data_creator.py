def clean_string(x):
    return str(x).replace("\n", " ").replace("\r", " ").replace("'", "").replace("\"", "")

def df_to_ddl(df, df_name="df", use_parallelize=False):

    pdf = df.toPandas()
    
    
    
    l = []
    for r in pdf.to_dict("records"):
        l.append("Row(" + ", ".join([f"{k}='{clean_string(v)}'" for k, v in r.items()]) + ")")
    msg = "\n"+",\n".join(l)


    if use_parallelize:
        msg = f"""\n
        {df_name} = spark.sparkContext.parallelize(\n
            [
                {msg}
            ]
        ).toDF()
        """
    else:
        msg = f"""\n
        {df_name} = spark.createDataFrame(\n
            [
                {msg}
            ]
        )
        """

    return print(msg)


def pdf_to_ddl(pdf, pdf_name="pdf"):
    print (f"{pdf_name} = pd.DataFrame( %s )" % (str(pdf.to_dict()).replace(" nan"," float('nan')")))

pdf_to_ddl(df, pdf_name="pdf_fit_summary")

df_to_ddl(df_trx_raw_custom, df_name="df_trx_raw_test", use_parallelize=True)

df_trx_raw = df_trx_raw.join(df_location, on=["RETAIL_OUTLET_LOCATION_SK"], how="left")
        .join(df_product, on=["item_sk"], how="left")

df_trx_raw_processed_custom = df_trx_raw.filter(F.col("STORE_NO").isin(exclusion_list)).limit(3)
.unionAll(df_trx_raw.filter(~F.col("STORE_NO").isin(exclusion_list)).limit(3))
.unionAll(df_trx_raw.filter(F.col("STORE_NO").isin(store_replace)).limit(3))
.unionAll(df_trx_raw.filter(F.col("STORE_NO")==store_no_replace[1]).limit(3))


df_location_custom = df_location_custom.withColumn("VALID_FROM_DTTM",F.to_date(F.col("VALID_FROM_DTTM")))
df_location_custom = df_location_custom.withColumn("VALID_TO_DTTM",F.to_date(F.col("VALID_TO_DTTM")))

df_trx_raw_processed_custom = df_trx_raw.filter(F.col("STORE_NO").isin(exclusion_list)).limit(3).unionAll(df_trx_raw.filter(~F.col("STORE_NO").isin(exclusion_list)).limit(3)).unionAll(df_trx_raw.filter(F.col("STORE_NO").isin(store_replace)).limit(3)).unionAll(df_trx_raw.filter(F.col("STORE_NO")==store_no_replace[1]).limit(3))

