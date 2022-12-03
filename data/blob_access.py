import pandas as pd
import os


def get_pd_df(url, schema):
    pd.set_option('display.max_columns', None)
    pd_df = pd.read_csv(url, header=0, names=schema)
    return pd_df

def get_three_df(urls, schemas):
    three_df = []
    for index, url in enumerate(urls):
        three_df.append(get_pd_df(url, schemas[index]))
    return three_df

def get_joined_df(pd_df_arr):
    first_join = pd_df_arr[2].merge(pd_df_arr[0], how="inner", on="HouseholdNum")
    second_join = first_join.merge(pd_df_arr[1], how="inner", on="ProductNum")
    return second_join

    

def run(num):

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
    joined_df["HouseholdNum"] = pd.to_numeric(joined_df["HouseholdNum"])
    filtered_df = joined_df[(joined_df.HouseholdNum == num)]
    table = filtered_df[["HouseholdNum","BasketNum","Date","ProductNum","Department","Commodity","Spend","Units",
    "StoreRegion", "WeekNum", "Year", "Loyalty", "AgeRange", "Marital", "IncomeRange", "Homeowner", "Composition",
    "Size", "Children"]].sort_values(by=["BasketNum", "Date", "ProductNum", "Department", "Commodity"])

    return table.head(5000).values.tolist()

def run_files(files, num):

    file_paths = []
    for file in files:
        file_paths.append(os.path.join(os.getcwd(), "data/uploads/" + file))
    # for file in file_paths:
    #     print(file)
    # print(num)

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

    joined_df = get_joined_df(get_three_df(file_paths, schemas))
    joined_df["HouseholdNum"] = pd.to_numeric(joined_df["HouseholdNum"])
    filtered_df = joined_df[(joined_df.HouseholdNum == num)]
    table = filtered_df[["HouseholdNum","BasketNum","Date","ProductNum","Department","Commodity","Spend","Units",
    "StoreRegion", "WeekNum", "Year", "Loyalty", "AgeRange", "Marital", "IncomeRange", "Homeowner", "Composition",
    "Size", "Children"]].sort_values(by=["BasketNum", "Date", "ProductNum", "Department", "Commodity"])

    return table.head(5000).values.tolist()

if __name__ == "__main__":
    run(10)