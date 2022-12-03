import pandas as pd
import os


def get_pd_df(url, schema):
    pd.set_option('display.max_columns', None)
    pd_df = pd.read_csv(url, header=0, names=schema)
    print(pd_df.head())
    return pd_df

def get_three_df(urls, schemas):
    three_df = []
    for index, url in enumerate(urls):
        three_df.append(get_pd_df(url, schemas[index]))
    return three_df

def get_joined_df(pd_df_arr):
    return pd.concat(
    pd_df_arr,
    axis=0,
    join="outer",
    ignore_index=False,
    keys=None,
    levels=None,
    names=None,
    verify_integrity=False,
    copy=True,
    )

def get_table(df):
    

# def get_combined_df(sqlContext, sparkSchemas, urls):
#     household_pd_df = get_pd_df(urls[0])
#     products_pd_df = get_pd_df(urls[1])#.head(5000)

#     # get 10k entries from dataframe
#     transactions_pd_df = get_pd_df(urls[2])#.head(10000)

#     household_df = sqlContext.createDataFrame(household_pd_df, schema=sparkSchemas[0])
#     products_df = sqlContext.createDataFrame(products_pd_df, schema=sparkSchemas[1])
#     transactions_df = sqlContext.createDataFrame(transactions_pd_df, schema=sparkSchemas[2])
#     return [household_df, products_df, transactions_df]

def run():

    # householdSchema = StructType([
    #     StructField("HouseholdNum", IntegerType(), True),
    #     StructField("Loyalty", StringType(), True),
    #     StructField("AgeRange", StringType(), True),
    #     StructField("Marital", StringType(), True),
    #     StructField("IncomeRange", StringType(), True),
    #     StructField("Homeowner", StringType(), True),
    #     StructField("Composition", StringType(), True),
    #     StructField("Size", StringType(), True),
    #     StructField("Children", StringType(), True)
    # ])
    

    # productsSchema = StructType([
    #     StructField("ProductNum", IntegerType(), True),
    #     StructField("Department", StringType(), True),
    #     StructField("Commodity", StringType(), True),
    #     StructField("BrandType", StringType(), True),
    #     StructField("Organic", StringType(), True)
    # ])

    

    # transactionsSchema = StructType([
    #     StructField("BasketNum", IntegerType(), True),
    #     StructField("HouseholdNum", IntegerType(), True),
    #     StructField("Date", StringType(), True),
    #     StructField("ProductNum", IntegerType(), True),
    #     StructField("Spend", FloatType(), True),
    #     StructField("Units", IntegerType(), True),
    #     StructField("StoreRegion", StringType(), True),
    #     StructField("WeekNum", IntegerType(), True),
    #     StructField("Year", IntegerType(), True)
    # ])

    

    # sparkSchemas = [householdSchema, productsSchema, transactionsSchema]

    urls = [
        "https://datastorage192.blob.core.windows.net/newcontainer/"\
        "400_households.csv?sv=2021-08-06&st=2022-11-27T04%3A26%3A14Z&se=2269-07-09T0"\
        "3%3A26%3A00Z&sr=b&sp=r&sig=rloizBi2H%2BCCzNR5TCgIc81j8dxrqLGdWCwH71zZ%2Fg8%3D",
        "https://datastorage192.blob.core.windows.net/newcontainer/"\
        "400_products.csv?sv=2021-08-06&st=2022-11-27T04%3A27%3A07Z&se=2305-07-07T03"\
        "%3A27%3A00Z&sr=b&sp=r&sig=J2nyr9z8LwXnjwzulVe09OJ%2FmZScrE4jFwldn2%2BA63U%3D",
        "https://datastorage192.blob.core.windows.net/newcontainer/"\
        "400_transactions.csv?sv=2021-08-06&st=2022-11-27T04%3A28%3A03Z&se=2289-12-19T04"\
        "%3A28%3A00Z&sr=b&sp=r&sig=TyPvbeQp5PJ7%2F%2F3F8jWacJhon3%2BqP3ieMwd%2Frt3NAJ4%3D"
        # "https://datastorage192.blob.core.windows.net/newcontainer/"\
        # "400_transactions_short.csv?sv=2021-08-06&st=2022-11-28T01%"\
        # "3A17%3A42Z&se=2304-04-29T00%3A17%3A00Z&sr=b&sp=r&sig=OJ1dN"\
        # "q2QC86j%2BIGJxPPEizlrM1IRtVoUdL%2B3WAoE0Kc%3D"
    ]

    householdSchema = [
        "HouseholdNum",
        "Loyalty",
        "AgeRange",
        "Marital",
        "IncomeRange",
        "Homeowner",
        "Composition",
        "Size",
        "Children"
    ]

    productSchema = [
        "ProductNum",
        "Department",
        "Commodity",
        "BrandType",
        "Organic"
    ]

    transactionSchema = [
        "BasketNum",
        "HouseholdNum",
        "Date",
        "ProductNum",
        "Spend",
        "Units",
        "StoreRegion",
        "WeekNum",
        "Year"
    ]

    schemas = [householdSchema, productSchema, transactionSchema]

    joined_df = get_joined_df(get_three_df(urls, schemas))

    print(joined_df)
    # combined_df = get_combined_df(sqlContext, sparkSchemas, urls)
    #combined_df[2].show()

    # productNumJoin_df = combined_df[2].join(combined_df[1], combined_df[2].ProductNum == combined_df[1].ProductNum)
    # full_df = productNumJoin_df.join(combined_df[0], productNumJoin_df.HouseholdNum == combined_df[0].HouseholdNum).filter(combined_df[0].HouseholdNum == num).select(combined_df[0].HouseholdNum,
    # "BasketNum", "Date", combined_df[1].ProductNum, "Department", "Commodity", "Spend", "Units", "StoreRegion", "WeekNum", "Year", "Loyalty",
    # "AgeRange", "Marital", "IncomeRange", "Homeowner", "Composition", "Size", "Children")


    # full_sorted_df = full_df.sort("BasketNum", "Date",
    # combined_df[1].ProductNum, "Department", "Commodity").head(5000)

    # return full_sorted_df

# def run_file(sparkContext, filenames, num):
#     sqlContext = SQLContext(sparkContext)

#     householdSchema = StructType([
#         StructField("HouseholdNum", IntegerType(), True),
#         StructField("Loyalty", StringType(), True),
#         StructField("AgeRange", StringType(), True),
#         StructField("Marital", StringType(), True),
#         StructField("IncomeRange", StringType(), True),
#         StructField("Homeowner", StringType(), True),
#         StructField("Composition", StringType(), True),
#         StructField("Size", StringType(), True),
#         StructField("Children", StringType(), True)
#     ])

#     productsSchema = StructType([
#         StructField("ProductNum", IntegerType(), True),
#         StructField("Department", StringType(), True),
#         StructField("Commodity", StringType(), True),
#         StructField("BrandType", StringType(), True),
#         StructField("Organic", StringType(), True)
#     ])

#     transactionsSchema = StructType([
#         StructField("BasketNum", IntegerType(), True),
#         StructField("HouseholdNum", IntegerType(), True),
#         StructField("Date", StringType(), True),
#         StructField("ProductNum", IntegerType(), True),
#         StructField("Spend", FloatType(), True),
#         StructField("Units", IntegerType(), True),
#         StructField("StoreRegion", StringType(), True),
#         StructField("WeekNum", IntegerType(), True),
#         StructField("Year", IntegerType(), True)
#     ])

#     sparkSchemas = [householdSchema, productsSchema, transactionsSchema]

#     urls = [
#         os.path.join(os.getcwd(), "uploads/" + filenames[0]),
#         os.path.join(os.getcwd(), "uploads/" + filenames[1]),
#         os.path.join(os.getcwd(), "uploads/" + filenames[2])
#     ]

#     print("TEST: " + os.getcwd(), "uploads/" + filenames[0])

#     combined_df = get_combined_df(sqlContext, sparkSchemas, urls)
#     #combined_df[2].show()

#     productNumJoin_df = combined_df[2].join(combined_df[1], combined_df[2].ProductNum == combined_df[1].ProductNum)
#     full_df = productNumJoin_df.join(combined_df[0], productNumJoin_df.HouseholdNum == combined_df[0].HouseholdNum).filter(combined_df[0].HouseholdNum == num).select(combined_df[0].HouseholdNum,
#     "BasketNum", "Date", combined_df[1].ProductNum, "Department", "Commodity", "Spend", "Units", "StoreRegion", "WeekNum", "Year", "Loyalty",
#     "AgeRange", "Marital", "IncomeRange", "Homeowner", "Composition", "Size", "Children")


#     full_sorted_df = full_df.sort("BasketNum", "Date",
#     combined_df[1].ProductNum, "Department", "Commodity").head(100)

#     return full_sorted_df


if __name__ == "__main__":
    # sparkContext = SparkContext.getOrCreate()
    # combined = run_file(sparkContext, ["households.csv", "products.csv", "transactions.csv"], 1600)
    # print(combined[0:10])

    run()