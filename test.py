import pandas as pd

df = pd.read_excel("D:\study\code\python\ETL\data\IMBD_Movie_Ratings.xlsx", engine="openpyxl")


# print(df.isna().count())

for i in df.columns:
    # print(i)
    df[i].fillna("NA", inplace=True)
    print("Successfully")

print("After process:\n", df.info())

