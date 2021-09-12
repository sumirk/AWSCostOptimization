import json
import boto3
import botocore
from pprint import pprint
import pandas as pd
import os
from os import path
from datetime import datetime
import numpy as np
import argparse
import uuid


parser = argparse.ArgumentParser()

parser.add_argument('--months',
                    dest='months',
                    help='Months to go back',
                    type=int,
                    nargs=1
                    )
args = parser.parse_args()

arg = args.months[0]

today = datetime.today()
decrMonth = datetime.today()
decrMonth = datetime.date(decrMonth.replace(day=1))
today = today.replace(month=today.month - arg)
startTime = datetime.date(today.replace(day=1))
endTime = datetime.date(today.replace(
    month=today.month + 1, day=1))
StartTimeSdk = datetime.date(
    today.replace(day=1)).strftime("%Y-%m-%d")
EndTimeSdk = decrMonth.replace(
    month=decrMonth.month + 1, day=1).strftime("%Y-%m-%d")
print({'today': today, 'decrMonth': decrMonth, 'startTime': startTime,
       'endTime': endTime, 'StartTimeSdk': StartTimeSdk, 'EndTimeSdk': EndTimeSdk})


client = boto3.client('ce', region_name='us-east-1')
clientcur = boto3.client('cur', region_name='us-east-1')


def convertSort(dataList, MyNewList, MyList):
    for dataDict in dataList:
        myList = dataDict['Groups']
        for index, dicts in enumerate(myList):
            for key in dicts:
                if key == 'Metrics':
                    dataDict['Groups'][index]['Metrics']['BlendedCost']['Amount'] = float(
                        dataDict['Groups'][index]['Metrics']['BlendedCost']['Amount'])
        MyList.append(dataDict)
    return MyList


response = client.get_cost_and_usage(
    TimePeriod={
        'Start': StartTimeSdk,
        'End': EndTimeSdk
    },
    Granularity='MONTHLY',
    Metrics=[
        'BlendedCost',
    ],
    GroupBy=[
        {
            'Type': 'DIMENSION',
            'Key': 'SERVICE'
        },
        {
            'Type': 'DIMENSION',
            'Key': 'USAGE_TYPE'
        }
    ]
    # NextPageToken='string'
)


allUsageList = response['ResultsByTime']
sortedServiceList = []
MyList_Service = []
testLists = convertSort(allUsageList, sortedServiceList, MyList_Service)

df = pd.json_normalize(testLists)

df_All_Grouped = pd.DataFrame(columns=["Service", "UsageType",
                                       "Metrics.BlendedCost.Amount"])


for index, content in df.iterrows():
    print(content)
    print(content['TimePeriod.Start'])
    print(type(content['TimePeriod.Start']))
    dfExt = pd.json_normalize(content['Groups'])
    dfExt.rename(columns={
                 'Metrics.BlendedCost.Amount': content['TimePeriod.Start']}, inplace=True)

    df_All_Grouped = pd.concat([df_All_Grouped, dfExt], ignore_index=True)


df_All_Grouped.Keys = df_All_Grouped.Keys.astype(str)

dfnew = pd.DataFrame(df_All_Grouped.Keys.str.split(',', 2).tolist(),
                     columns=['first', 'second'])


df_All_Grouped["Service"] = dfnew["first"]
df_All_Grouped["UsageType"] = dfnew["second"]

df_All_Grouped["Service"] = df_All_Grouped["Service"].str.replace(
    '[', '', regex=False)
df_All_Grouped["UsageType"] = df_All_Grouped["UsageType"].str.replace(
    ']', '', regex=False)
df_All_Grouped["UsageType"] = df_All_Grouped["UsageType"].str.replace(
    "'", '', regex=False)
df_All_Grouped["Service"] = df_All_Grouped["Service"].str.replace(
    "'", '', regex=False)

# By Usage Type Sheet
df_All_Grouped_group = df_All_Grouped.groupby([
    'Service', 'UsageType'], as_index=False)
agg_df_All_Grouped = df_All_Grouped_group.aggregate(np.sum)

sortedcolumn = list(df['TimePeriod.Start'])
sortedcolumn.reverse()

sorted_agg_df_All_Grouped = agg_df_All_Grouped.sort_values(
    sortedcolumn, ascending=False)

# Data transfer Sheet
dfData = sorted_agg_df_All_Grouped[sorted_agg_df_All_Grouped.UsageType.str.contains(
    "Bytes")]

# Copying original df_All_Grouped to create a new sheet with service

g = df_All_Grouped.groupby('Service', as_index=False)
gsum = g.sum()

# By-Service Sheet
gsorted = gsum.sort_values(list(df['TimePeriod.Start']), ascending=False)


suggestDict = {'Amazon Elastic Compute Cloud - Compute': {'BoxUsage': 'RightSize the instance --> Check Spot --> Check AMD migration --> Check Graviton migration -- Savings Plan'},
               'Amazon Relational Database Service': {'InstanceUsage': 'For production DB move to Graviton --> Reserve the DB', 'GP2': 'Migrate Storage to GP3 to save cost'}, 'Amazon Kinesis': {'ShardHour': 'Check metrics per shard to verify Utilization'}}

df_All_GroupedNew = pd.DataFrame(columns=["Service", "UsageType",
                                          "Metrics.BlendedCost.Unit", "Suggestions", "Implementation Effort", "Impact on cost saving"])
for i in gsorted.Service:
    groupDf = g.get_group(i)
    # print('*******************************')

    # Taking Assumption of 20$
    # print(groupDf_filtered.head(2))
    try:
        if i in suggestDict.keys():
            for row in groupDf.itertuples():
                for usage, value in suggestDict[i].items():
                    if usage in row.UsageType:
                        # print(row.UsageType)
                        groupDf.at[row.Index, "Suggestions"] = value
    except Exception as e:
        continue
    df_All_GroupedNew = pd.concat(
        [df_All_GroupedNew, groupDf], ignore_index=True)

# By-Grouped-Service Sheet
df_All_GroupedNew_grouped = df_All_GroupedNew.groupby(
    ["Service", "UsageType"], as_index=False)


df_All_GroupedNew_grouped_agg = df_All_GroupedNew_grouped.aggregate(np.sum).sort_values(
    list(df['TimePeriod.Start']), ascending=False)


######### For Account Based API Usage Type data ###############################


response_account = client.get_cost_and_usage(
    TimePeriod={
        'Start': StartTimeSdk,
        'End': EndTimeSdk
    },
    Granularity='MONTHLY',
    Metrics=[
        'BlendedCost',
    ],
    GroupBy=[
        {
            'Type': 'DIMENSION',
            'Key': 'LINKED_ACCOUNT'
        },
        {
            'Type': 'DIMENSION',
            'Key': 'USAGE_TYPE'
        }
    ]
    # NextPageToken='string'
)


allUsageList_account = response_account['ResultsByTime']

sortedAccountList_account = []

MyList_Account = []

testLists_account = convertSort(
    allUsageList_account, sortedAccountList_account, MyList_Account)


df_account = pd.json_normalize(testLists_account)


df_All_Grouped_account = pd.DataFrame(columns=["Account", "UsageType",
                                               "Metrics.BlendedCost.Amount"])

for index, content in df_account.iterrows():
    print(content)
    print(content['TimePeriod.Start'])
    print(type(content['TimePeriod.Start']))
    dfExt = pd.json_normalize(content['Groups'])
    dfExt.rename(columns={
                 'Metrics.BlendedCost.Amount': content['TimePeriod.Start']}, inplace=True)

    df_All_Grouped_account = pd.concat(
        [df_All_Grouped_account, dfExt], ignore_index=True)


df_All_Grouped_account.Keys = df_All_Grouped_account.Keys.astype(str)


dfnew_account = pd.DataFrame(df_All_Grouped_account.Keys.str.split(',', 2).tolist(),
                             columns=['first', 'second'])


df_All_Grouped_account["Account"] = dfnew_account["first"]
df_All_Grouped_account["UsageType"] = dfnew_account["second"]

df_All_Grouped_account["Account"] = df_All_Grouped_account["Account"].str.replace(
    '[', '', regex=False)
df_All_Grouped_account["UsageType"] = df_All_Grouped_account["UsageType"].str.replace(
    ']', '', regex=False)
df_All_Grouped_account["UsageType"] = df_All_Grouped_account["UsageType"].str.replace(
    "'", '', regex=False)
df_All_Grouped_account["Account"] = df_All_Grouped_account["Account"].str.replace(
    "'", '', regex=False)

df_All_Grouped_account_group = df_All_Grouped_account.groupby([
    'Account', 'UsageType'], as_index=False)

agg_df_All_Grouped_account = df_All_Grouped_account_group.aggregate(np.sum)

sortedcolumn_account = list(df_account['TimePeriod.Start'])
sortedcolumn_account.reverse()

sorted_agg_df_All_Grouped_account = agg_df_All_Grouped_account.sort_values(
    sortedcolumn_account, ascending=False)


######## Generate the CUR data ##########
responseCur = clientcur.describe_report_definitions()
responseCur = responseCur['ReportDefinitions']


req_cols = ['lineItem/UsageAccountId', 'product/ProductName', 'product/productFamily', 'product/servicecode',
            'lineItem/UsageType', 'lineItem/ResourceId', 'lineItem/BlendedCost', 'product/region', 'pricing/term', 'pricing/unit']

# req_cols = ['lineItem/UsageAccountId', 'product/ProductName', 'product/productFamily', 'product/servicecode',
#             'lineItem/UsageType', 'lineItem/ResourceId', 'lineItem/BlendedCost', 'product/region', 'pricing/term']


def readFile(fileName):
    dfTemp = pd.read_csv(fileName, compression='gzip', header=0, index_col=0, quotechar='"', sep=',',
                         na_values=['na', '-', '.', ''], usecols=req_cols)

    dfTemp.rename(
        columns={'lineItem/BlendedCost': dateRange + '-cost'}, inplace=True)
    return dfTemp


if len(responseCur) > 0:
    for config in responseCur:
        if 'RESOURCES' in config['AdditionalSchemaElements'] and config['ReportVersioning'] == 'OVERWRITE_REPORT' and config['Compression'] == 'GZIP':
            bucketname = config['S3Bucket']
            pathname = './'
            s3 = boto3.resource('s3')
            if not path.exists("./"):
                try:
                    os.makedirs(pathname)
                except OSError:
                    print("Creation of the directory %s failed" % path)
            token = str(uuid.uuid4())

            fileDict = {}

            while decrMonth.month - startTime.month >= 0:
                dateRange = decrMonth.strftime("%Y%m%d") + \
                    '-' + \
                    decrMonth.replace(
                        month=decrMonth.month + 1).strftime("%Y%m%d")
                prefix = '/' + config['ReportName'] + '/' + dateRange + '/' + \
                    config['ReportName'] + '-00001.csv.gz'

                localFile = token + '-' + dateRange + '.csv.gz'
                print(prefix)
                print({'today': today, 'decrMonth': decrMonth, 'startTime': startTime,
                       'endTime': endTime, 'StartTimeSdk': StartTimeSdk, 'EndTimeSdk': EndTimeSdk})
                try:
                    s3.Bucket(bucketname).download_file(prefix, localFile)
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")
                    else:
                        raise
                decrMonth = decrMonth.replace(month=decrMonth.month - 1)
                fileDict[dateRange] = localFile


def readFile(rangeName, fileName):
    dfTemp = pd.read_csv(fileName, compression='gzip', header=0,
                         quotechar='"', sep=',', na_values=['na', '-', '.', ''], usecols=req_cols)
    dfTemp.rename(columns={'lineItem/BlendedCost': rangeName}, inplace=True)
    return dfTemp


dfCur = pd.concat([readFile(key, value)
                   for key, value in fileDict.items()])

group_df = dfCur.groupby(['product/ProductName', 'product/productFamily',
                          'lineItem/UsageType', 'lineItem/ResourceId', 'pricing/unit'], as_index=False)

group_df_Account = dfCur.groupby(['lineItem/UsageAccountId', 'product/ProductName', 'product/productFamily',
                                  'lineItem/UsageType', 'lineItem/ResourceId', 'pricing/unit'])

agg_df = group_df.aggregate(np.sum)
agg_df_Account = group_df_Account.aggregate(np.sum)


sorted_agg_df = agg_df.sort_values(list(fileDict.keys()), ascending=False)

sorted_agg_df_account = agg_df_Account.sort_values(
    list(fileDict.keys()), ascending=False)


# Write all the Dataframes to sheets
sheets = {'By-Usage-Type': sorted_agg_df_All_Grouped, 'By-Account-Usage': sorted_agg_df_All_Grouped_account,
          'By-Service': gsorted, 'By-Grouped-Service': df_All_GroupedNew_grouped_agg, 'Data-Transfer-PerService': dfData, 'ByResource': sorted_agg_df, 'ByAccount': sorted_agg_df_account}
writer = pd.ExcelWriter('./costexplorerSheets.xlsx', engine='xlsxwriter')


for sheet_name in sheets.keys():
    sheets[sheet_name].to_excel(writer, sheet_name=sheet_name)

writer.save()
# Implement a CPU utilization per resource type and with their cost as well like Average CPu and Peak CPU, can also check how many hours it ran
