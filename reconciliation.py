import shutil
import sys
import pyodbc
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, time, timedelta
import pytz
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os
import math
import concurrent.futures
from io import StringIO
 
# ---- Table Mappings ----
table_mappings = [
    {
        "name": "LegacyAcn",
        "df_table": "ccna-fab-prod-LegacyAcn",
        "mdm_table": "V_DF_C_BO_LEGACY_ACN",
        "mdm_filter": "REC_UPDT_DT_ACN",
        "df_key_condition_expression": "acnLegacySrc = :acnSrc AND recUpdateDateAcn BETWEEN :sdt AND :edt",
        "df_sort_key_values": [{"0": "FS"}],
        "df_index": "BYRECUPDATEDATE1",
        "partition_key": ":acnSrc",
        "merge_key": "acn",
        "mdm_columns": "ZLKUNNR as acn, LAST_UPDATE_DATE as lastUpdateDate, FTN_FTN_MKT_ID, REC_UPDT_DT_ACN, REC_UPDT_TM_ACN"
    },
    {
        "name": "Customer360Hierarchy",
        "df_table": "ccna-fab-prod-Customer360Hierarchy",
        "mdm_table": "V_DF_CUSTOMER360_Hierarchy",
        "mdm_filter": "Last_Update_Date",
        "df_key_condition_expression": "hierarchySource = :hierarchySource AND lastUpdateDate BETWEEN :sdt AND :edt",
        "df_sort_key_values": [
            {"0": "Concessionaire"},
            {"1": "FSOP"},
            {"2": "Mngd BY"},
            {"3": "NABDB"},
            {"4": "WAREHOUSE"}
        ],
        "df_index": "BYLASTUPDATEDATE",
        "partition_key": ":hierarchySource",
        "merge_key": "uniqueId",
        "mdm_columns": "UNIQUE_ID as uniqueId, LAST_UPDATE_DATE as lastUpdateDate, DIVISION, NODE_ID as nodeId, PARENT_NODE_ID as parentNodeId, NODE_LEVEL_NAME as nodeLevelName"
    },
    {
        "name": "Customer360Acn",
        "df_table": "ccna-fab-prod-Customer360Acn",
        "mdm_table": "V_DF_CUSTOMER360_ACN",
        "mdm_filter": "ACN_Record_Update_Date",
        "df_key_condition_expression": "acnLegacySource = :acnSrc AND acnRecordUpdateDate BETWEEN :sdt AND :edt",
        "df_sort_key_values": [{"0": "FS"}],
        "df_index": "BYLASTUPDATEDATE",
        "partition_key": ":acnSrc",
        "merge_key": "uniqueId",
        "mdm_columns": "UNIQUE_ID as uniqueId, LAST_UPDATE_DATE as lastUpdateDate, FTN_FTN_MKT_NM, ACN_Record_Update_Date, ACN_Record_Update_Time"
    },
    {
        "name": "Customer360Channel",
        "df_table": "ccna-fab-prod-Customer360Channel",
        "mdm_table": "V_DF_CUSTOMER360_CHANNEL",
        "mdm_filter": "Last_Update_Date",
        "df_key_condition_expression": "co = :co AND lastUpdateDate BETWEEN :sdt AND :edt",
        "df_sort_key_values": [
            {"0": "2"},
            {"1": "3"},
            {"2": "4"},
            {"3": "5"},
            {"4": "99"}
        ],
        "df_index": "BYLASTUPDATEDATE",
        "partition_key": ":co",
        "merge_key": "uniqueId",
        "mdm_columns": "UNIQUE_ID as uniqueId, LAST_UPDATE_DATE as lastUpdateDate, SC as sc, TC as tc, TRADE_CHANNEL as tradeChannel, AC as ac, ACTIVITY_CLUSTER as activityCluster, CO as co, CHANNEL_ORG as channelOrg, SUB_TRADE_CHANNEL as subTradeChannel"
    },
    # {
    #     "name": "Customer360HierMap",
    #     "df_table": "ccna-fab-prod-Customer360HierMap",
    #     "mdm_table": "V_DF_CUSTOMER360_HIER_MAP",
    #     "mdm_filter": "Last_Update_Date",
    #     "df_key_condition_expression": "hierarchySource = :hierarchySource AND lastUpdateDate BETWEEN :sdt AND :edt",
    #     "df_sort_key_values": [
    #         {"0": "WAREHOUSE"},
    #         {"1": "NABDB"},
    #         {"2": "Mngd BY"},
    #         {"3": "Concessionaire"},
    #         {"4": "FSOP"}
    #     ],
    #     "df_index": "BYLASTUPDATEDATE",
    #     "partition_key": ":hierarchySource",
    #     "merge_key": "uniqueId",
    #     "mdm_columns": "UNIQUE_ID as uniqueId, LAST_UPDATE_DATE as lastUpdateDate, FSOP_ID as fsopId, FSOP_NAME as fsopName, CBS_ACCOUNT_NO as cbsAccountNo, CBS_ACCOUNT_NAME as cbsAccountName, FSOP_MAPPING_LEVEL as fsopMappingLevel, CBS_MAPPING_LEVEL as cbsMappingLevel, NABDB_PE_ID as nabdbPeId, NABDB_PE_NAME as nabdbPeName, HIERARCHY_SOURCE as hierarchySource, MAPPING_DECISION as mappingDecision, ORPHAN_OUTLET_FLAG as orphanOutletFlag, FSOP_BP_ID as fsopBpId, LINE_OF_BUSINESS as lineOfBusiness"
    # },
    {
        "name": "Customer360Outlet",
        "df_table": "ccna-fab-prod-Customer360Outlet",
        "mdm_table": "V_DF_CUSTOMER360_OUTLET",
        "mdm_filter": "Last_Update_Date",
        "df_key_condition_expression": "stdCountryCode = :stdCountryCode AND lastUpdateDate BETWEEN :sdt AND :edt",
        "df_sort_key_values": [
            {"0": "CA"},
            {"1": "US"},
            {"2": "AU"},
            {"3": "PE"},
            {"4": "PR"},
            {"5": "GU"},
            {"6": "VI"}
        ],
        "df_index": "BYLASTUPDATEDATE",
        "partition_key": ":stdCountryCode",
        "merge_key": "uniqueId",
        "mdm_columns": "UNIQUE_ID as uniqueId, LAST_UPDATE_DATE as lastUpdateDate, CUST_NBR as customerNumber, PARENT_NODE_ID as parentNodeId, GOLDEN_RECORD_ID as goldenRecordId"
    },
    {
        "name": "Customer360OutletDel",
        "df_table": "ccna-fab-prod-Customer360OutletDel",
        "mdm_table": "V_DF_CUSTOMER360_OUTLET_DEL",
        "mdm_filter": "Last_Update_Date",
        "df_key_condition_expression": "stdCountryCode = :stdCountryCode AND lastUpdateDate BETWEEN :sdt AND :edt",
        "df_sort_key_values": [
            {"0": "CA"},
            {"1": "PR"},
            {"2": "US"}
        ],
        "df_index": "BYLASTUPDATEDATE",
        "partition_key": ":stdCountryCode",
        "merge_key": "uniqueId",
        "mdm_columns": "UNIQUE_ID as uniqueId, LAST_UPDATE_DATE as lastUpdateDate, CUST_NBR as customerNumber, PARENT_NODE_ID as parentNodeId, OWNRSHP_ID as ownrshpID, OWNRSHP_ID_NM as ownrshpIDName, GOLDEN_RECORD_ID as goldenRecordId, STD_OPERATIONAL_NAME as stdOperationalName, STD_ADDR_LN1 as stdAddrLine1, STD_CITY as stdCity, STD_ZIP_CD as stdZipCode, STD_STATE_CD as stdStateCode, STD_COUNTRY_CD as stdCountryCode, STORE_NBR as storeNumber, MNGD_BY_CBS_ACCT_NBR as managedByCbsAcctNBR, CBS_TRADE_NAME as cbsTradeName, SALES_PSTN_ID as salesPstnId, SALES_PERSON_ID as salesPersonId, REV_CENTER_ID as revCenterId, REV_CENTER_NAME as revCenterName"
    },
    {
        "name": "Customer360HierarchyDel",
        "df_table": "ccna-fab-prod-Customer360HierarchyDel",
        "mdm_table": "V_DF_CUSTOMER360_HIERARCHY_DEL",
        "mdm_filter": "Last_Update_Date",
        "df_key_condition_expression": "hierarchySource = :hierarchySource AND lastUpdateDate BETWEEN :sdt AND :edt",
        "df_sort_key_values": [
            {"0": "Concessionaire"},
            {"1": "FSOP"},
            {"2": "Mngd BY"},
            {"3": "NABDB"},
            {"4": "WAREHOUSE"}
        ],
        "df_index": "BYLASTUPDATEDATE",
        "partition_key": ":hierarchySource",
        "merge_key": "uniqueId",
        "mdm_columns": "UNIQUE_ID as uniqueId, LAST_UPDATE_DATE as lastUpdateDate, NODE_ID as nodeId, PARENT_NODE_ID as parentNodeId, NODE_LEVEL_NAME as nodeLevelName, NODE_DESCRIPTION as nodeDescription, STREET as street, CITY as city, STATE_CD as stateCode, ZIP as zip, COUNTRY_CD as countryCode, BP_NUMBER as bpNumber, BP_VALID_FROM as bpValidFrom, BP_VALID_TO as bpValidTo, HIER_IND_TYPE_CODE as hierIndTypeCode, SALES_ORG as salesOrg, DIVISION as division, DISTRIBUTION_CHNNL as distributionChannel, HIERARCHY_SOURCE as hierarchySource, LINE_OF_BUSINESS as lineOfBusiness, SUB_TRADE_CHANNEL as subTradeChannel, CUSTOMER_ID as customerId, STATUS as status, STATUS_REASON as statusReason"
    }
]
 
# The `table_mappings` list now contains all the required table configurations exactly as defined in the data provided.
# ---- Email Config ----
session_email = boto3.Session(profile_name='CCNA_INTSRVC_NonProd_INT_AppAdmin', region_name='us-west-2')
client_email = session_email.client('ses')
SENDER = "no-reply@coca-cola.com"
RECIPIENT = "cgdatafabricsupport@coca-cola.com"
#RECIPIENT = "ldendukuri@coca-cola.com"
CHARSET = "UTF-8"
 
# ---- Timeframe ----tReconciliation
eastern = pytz.timezone("America/New_York")
yesterday_dt = datetime.now(eastern).date() - timedelta(days=1)
naive_start = datetime.combine(yesterday_dt, time.min)
naive_end = datetime.combine(yesterday_dt, time.max)
yesterday_start_dt = eastern.localize(naive_start)
yesterday_end_dt = eastern.localize(naive_end)
format1 = "%Y-%m-%d %H:%M:%S"
format2 = "%Y-%m-%dT%H:%M:%S.%f"
format3 = "%Y-%m-%d"
del_table_df_counts = {}
 
def compute_main_table_del_counts():
    import boto3
    import pandas as pd
 
    global del_table_df_counts
 
    session1 = boto3.Session(profile_name='CCNA_SharedServices_FAB_AppAdmin', region_name='us-west-2')
    dynamodb = session1.resource('dynamodb')
 
    main_configs = [
        {
            "main_name": "Customer360Outlet",
            "del_name": "Customer360OutletDel",
            "df_table": "ccna-fab-prod-Customer360Outlet",
            "partition_key": ":stdCountryCode",
            "df_sort_key_values": [
                {"0": "CA"},
                {"1": "US"},
                {"2": "AU"},
                {"3": "PE"},
                {"4": "PR"},
                {"5": "GU"},
                {"6": "VI"}
            ],
            "df_index": "BYLASTUPDATEDATE",
            "df_key_condition_expression": "stdCountryCode = :stdCountryCode AND lastUpdateDate BETWEEN :sdt AND :edt"
        },
        {
            "main_name": "Customer360Hierarchy",
            "del_name": "Customer360HierarchyDel",
            "df_table": "ccna-fab-prod-Customer360Hierarchy",
            "partition_key": ":hierarchySource",
            "df_sort_key_values": [
                {"0": "Concessionaire"},
                {"1": "FSOP"},
                {"2": "Mngd BY"},
                {"3": "NABDB"},
                {"4": "WAREHOUSE"}
            ],
            "df_index": "BYLASTUPDATEDATE",
            "df_key_condition_expression": "hierarchySource = :hierarchySource AND lastUpdateDate BETWEEN :sdt AND :edt"
        }
    ]
 
    for cfg in main_configs:
        fab_table = dynamodb.Table(cfg["df_table"])
        total_not_a = 0
        for partition_value in cfg["df_sort_key_values"]:
            for sort_key in partition_value.values():
                expr_attr = {
                    cfg["partition_key"]: sort_key,
                    ':sdt': yesterday_start_dt.strftime(format2)[:-3] + 'Z',
                    ':edt': yesterday_end_dt.strftime(format2)[:-3] + 'Z'
                }
                response = fab_table.query(
                    IndexName=cfg["df_index"],
                    KeyConditionExpression=cfg["df_key_condition_expression"],
                    ExpressionAttributeValues=expr_attr,
                    Limit=75000
                )
                items = response.get('Items', [])
                flag_series = pd.Series([item.get('c360DelFlag', '').strip().upper() for item in items])
                count_not_a = (flag_series == 'D').sum()
                total_not_a += count_not_a
                while 'LastEvaluatedKey' in response:
                    response = fab_table.query(
                        IndexName=cfg["df_index"],
                        KeyConditionExpression=cfg["df_key_condition_expression"],
                        ExpressionAttributeValues=expr_attr,
                        Limit=75000,
                        ExclusiveStartKey=response['LastEvaluatedKey']
                    )
                    items = response.get('Items', [])
                    flag_series = pd.Series([item.get('c360DelFlag', '').strip().upper() for item in items])
                    count_not_a = (flag_series == 'D').sum()
                    total_not_a += count_not_a
        del_table_df_counts[cfg["del_name"]] = total_not_a
        print(f"[PRECOUNT] {cfg['main_name']} records with c360DelFlag = 'A' in last 24h: {total_not_a}")
 
compute_main_table_del_counts()
 
# ---- Excel Splitting Utility ----
def split_and_write_excel(dfs_dict, base_filename, max_rows=1000000):
    files = []
    max_len = max(df.shape[0] for df in dfs_dict.values())
    num_chunks = math.ceil(max_len / max_rows)
    for chunk_idx in range(num_chunks):
        if num_chunks == 1:
            fname = f"{base_filename}.xlsx"
        else:
            fname = f"{base_filename}_part{chunk_idx+1}.xlsx"
        with pd.ExcelWriter(fname) as writer:
            for sheet_name, df in dfs_dict.items():
                start = chunk_idx * max_rows
                end = min((chunk_idx + 1) * max_rows, df.shape[0])
                if start < end:
                    # Truncate sheet name to 31 chars for Excel
                    df.iloc[start:end].to_excel(writer, sheet_name=sheet_name[:31], index=False)
        files.append(fname)
    return files
 
# ---- Per-table Processing Function ----
def process_table(table_mapping):
    global del_table_df_counts
    import pyodbc
    import boto3
    import pandas as pd
    import math
    from datetime import datetime
 
    # Connections inside thread
    connection = pyodbc.connect(
        'Driver={SQL Server};'
        'Server=zwpmdmd5001.NA.KO.COM;'
        'Database=CMX_PUBLISH;'
        'UID=_datafabric_ro;'
        'PWD=Mdmpr#admin'
    )
    cursor = connection.cursor()
    session1 = boto3.Session(profile_name='CCNA_SharedServices_FAB_AppAdmin', region_name='us-west-2')
    dynamodb = session1.resource('dynamodb')
    ddb_client = session1.client('dynamodb')
 
    name = table_mapping["name"]
    print(f"\n[DEBUG] Processing {name}...")
 
    mdm_queryStartDate = yesterday_start_dt.strftime(format1)
    mdm_queryEndDate = yesterday_end_dt.strftime(format1)
    df_queryStartDate = yesterday_start_dt.strftime(format2)[:-3] + 'Z'
    df_queryEndDate = yesterday_end_dt.strftime(format2)[:-3] + 'Z'
 
    # ---- 1. Fetch DynamoDB Data First ----
    df_record = []
    fab_table = table_mapping['df_table']
    datafabric_table = dynamodb.Table(fab_table)
    df_total_record_count = 0
    df_resultset = []
    # Only fetch DynamoDB counts if not a Del view
    if not ("Del" in name):
        try:
            desc_tbl_response = ddb_client.describe_table(TableName=fab_table)
            df_total_record_count = desc_tbl_response['Table']['ItemCount']
            print(f"[DEBUG] DynamoDB total record count for {name}: {df_total_record_count}")
            for partition_value in table_mapping['df_sort_key_values']:
                for sort_key in partition_value.values():
                    expr_attr = {
                        table_mapping['partition_key']: sort_key,
                        ':sdt': df_queryStartDate,
                        ':edt': df_queryEndDate
                    }
                    # --- Fix for reserved keyword 'status' ---
                    if name == "Customer360PositionMarketId":
                        response = datafabric_table.query(
                            IndexName=table_mapping['df_index'],
                            KeyConditionExpression="#status = :status AND lastUpdateDate BETWEEN :sdt AND :edt",
                            ExpressionAttributeNames={"#status": "status"},
                            ExpressionAttributeValues=expr_attr,
                            Limit=75000
                        )
                    else:
                        response = datafabric_table.query(
                            IndexName=table_mapping['df_index'],
                            KeyConditionExpression=table_mapping['df_key_condition_expression'],
                            ExpressionAttributeValues=expr_attr,
                            Limit=75000
                        )
                    items = response.get('Items', [])
                    if name in ["Customer360Hierarchy", "Customer360Outlet"]:
                        items = [item for item in items if item.get('c360DelFlag') == 'A']
                    df_record.extend(items)
                    while 'LastEvaluatedKey' in response:
                        if name == "Customer360PositionMarketId":
                            response = datafabric_table.query(
                                IndexName=table_mapping['df_index'],
                                KeyConditionExpression="#status = :status AND lastUpdateDate BETWEEN :sdt AND :edt",
                                ExpressionAttributeNames={"#status": "status"},
                                ExpressionAttributeValues=expr_attr,
                                Limit=75000,
                                ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                        else:
                            response = datafabric_table.query(
                                IndexName=table_mapping['df_index'],
                                KeyConditionExpression=table_mapping['df_key_condition_expression'],
                                ExpressionAttributeValues=expr_attr,
                                Limit=75000,
                                ExclusiveStartKey=response['LastEvaluatedKey']
                            )
                        items = response.get('Items', [])
                        if name in ["Customer360Hierarchy", "Customer360Outlet"]:
                            items = [item for item in items if item.get('c360DelFlag') == 'A']
                        df_record.extend(items)
            print(f"[DEBUG] Fetched {len(df_record)} records from DynamoDB for {name}.")
            df_record_count_last_24hrs = len(df_record)
        except Exception as e:
            print(f"[ERROR] Fetching records from DynamoDB for {name}: {e}")
            return None
 
        df_resultset = [{
            'Table Name': fab_table,
            'Total Number of records': df_total_record_count,
            'Total Number of records in last 24Hrs': len(df_record),
            'NOW_TIME': datetime.now()
        }]
        print(f"[DEBUG] DynamoDB counts for {name}: {df_resultset}")
 
    if name in ["Customer360OutletDel", "Customer360HierarchyDel"]:
        global del_table_df_counts
        df_24hr_count = del_table_df_counts.get(name, 0)
        df_resultset = [{
            'Table Name': fab_table,
            'Total Number of records': df_total_record_count,
            'Total Number of records in last 24Hrs': df_24hr_count,
            'NOW_TIME': datetime.now()
        }]
        print(f"[DEBUG] DynamoDB counts for {name} (using precomputed c360DelFlag != 'A'): {df_resultset}")
    else:
        df_resultset = [{
            'Table Name': fab_table,
            'Total Number of records': df_total_record_count,
            'Total Number of records in last 24Hrs': len(df_record),
            'NOW_TIME': datetime.now()
        }]
        print(f"[DEBUG] DynamoDB counts for {name}: {df_resultset}")
 
    # ---- 2. Fetch MDM Data Next ----
    mdm_record = []
    mdm_resultset = []
    try:
        mdm_table = table_mapping['mdm_table']
        mdm_filter = table_mapping['mdm_filter']
        mdm_columns = table_mapping['mdm_columns']
        md_query = (
            f"SELECT {mdm_columns} "
            f"FROM [dbo].[{mdm_table}] ('1900-01-01 00:00:00','2030-01-01 00:00:00') "
            f"WHERE {mdm_filter} BETWEEN '{mdm_queryStartDate}' AND '{mdm_queryEndDate}'"
        )
        print(f"[DEBUG] Executing MDM query for {name}: {md_query}")
        cursor.execute(md_query)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        for row in rows:
            mdm_record.append(dict(zip(columns, row)))
        print(f"[DEBUG] Fetched {len(mdm_record)} records from MDM for {name}.")
 
        # Record counts
        total_query = f"SELECT count(1) FROM [dbo].[{mdm_table}] ('1900-01-01 00:00:00', '2030-01-01 00:00:00')"
        cursor.execute(total_query)
        mdm_total_record_count = cursor.fetchone()[0]
 
        hour_query = (
            f"SELECT count(1) FROM [dbo].[{mdm_table}] ('1900-01-01 00:00:00', '2030-01-01 00:00:00') "
            f"WHERE {mdm_filter} BETWEEN '{mdm_queryStartDate}' AND '{mdm_queryEndDate}'"
        )
        cursor.execute(hour_query)
        mdm_24hr_record_count = cursor.fetchone()[0]
 
        mdm_resultset.append({'TYPE': 'Full Table Count', 'RECORD_COUNT': mdm_total_record_count, 'TABLE_NAME': mdm_table})
        mdm_resultset.append({'TYPE': 'Last 24 Hours Record Count', 'RECORD_COUNT': mdm_24hr_record_count, 'TABLE_NAME': mdm_table})
        print(f"[DEBUG] MDM counts for {name}: {mdm_resultset}")
    except Exception as e:
        print(f"[ERROR] Fetching records from MDM for {name}: {e}")
        return None
 
    # ---- Prepare DataFrames ----
    current_datetime = datetime.now().strftime(format3)
    base_filename = f"ReconciliationReport_{name}_{current_datetime}"
    mdm_record_df = pd.DataFrame(data=mdm_record)
    df_record_df = pd.DataFrame(data=df_record)
 
    # ---- Ensure merge_key exists in both DataFrames ----
    merge_key = table_mapping["merge_key"]
    if merge_key not in mdm_record_df.columns:
        mdm_record_df[merge_key] = None
    if merge_key not in df_record_df.columns:
        df_record_df[merge_key] = None
 
    # Ensure merge_key is string in both DataFrames to avoid merge dtype errors
    mdm_record_df[merge_key] = mdm_record_df[merge_key].astype(str)
    df_record_df[merge_key] = df_record_df[merge_key].astype(str)
 
    # ---- Find Differences ----
    if mdm_record_df.empty and df_record_df.empty:
        diff_record = pd.DataFrame(columns=[merge_key, 'lastUpdateDate'])
    else:
        merged_df = pd.merge(
            mdm_record_df,
            df_record_df,
            on=merge_key,
            how='outer',
            indicator=True,
            suffixes=('_mdm', '_datafabric')
        )
        diff_record = merged_df[merged_df['_merge'] != 'both'].copy()
        diff_record['_merge'] = diff_record['_merge'].map({'left_only': 'in_mdm_only', 'right_only': 'in_datafabric_only'})
 
    # ---- Write Excel Files ----
    excel_files = split_and_write_excel(
        {
            f"MDM_{name}": mdm_record_df,
            f"FAB_{name}": df_record_df,
            f"DIFF_{name}": diff_record
        },
        base_filename
    )
    print(f"[DEBUG] Excel files for {name}: {excel_files}")
    all_excel_files.extend(excel_files)
    sharepoint_local_path = r"C:\Users\Z22461\The Coca-Cola Company\Data Fabric and ESP Support - DailyReconciliationReportDumps"
    for excel_file in excel_files:
        dest_path = os.path.join(sharepoint_local_path, os.path.basename(excel_file))
        try:
            # --- Delete existing file if present ---
            if os.path.exists(dest_path):
                os.remove(dest_path)
                print(f"[INFO] Deleted existing file at destination: {dest_path}")
            shutil.copy2(excel_file, dest_path)
            print(f"[INFO] Copied {excel_file} to SharePoint folder: {dest_path}")
        except Exception as e:
            print(f"[ERROR] Failed to copy {excel_file} to SharePoint: {e}")
 
    # ---- Prepare HTML for Email ----
    mdm_html1 = pd.DataFrame(data=mdm_resultset).to_html(index=False)
    if name not in ["Customer360OutletDel", "Customer360HierarchyDel"]:
        df_html1 = pd.DataFrame(data=df_resultset).to_html(index=False)
        html_section = f"""
        <h2 style="color:black">{name}</h2>
        <h3 style="color:black">Record Count in MDM View:</h3>
        <div>{mdm_html1}</div>
        <h3 style="color:black">Record Count in Data Fabric:</h3>
        <div>{df_html1}</div>
        <hr>
        """
    else:
        html_section = f"""
        <h2 style="color:black">{name}</h2>
        <h3 style="color:black">Record Count in MDM View:</h3>
        <div>{mdm_html1}</div>
        <hr>
        """
 
    # ---- Subject Color Logic ----
    mdm_count = mdm_resultset[1]['RECORD_COUNT'] if len(mdm_resultset) > 1 else 0
    df_count = df_resultset[0]['Total Number of records in last 24Hrs'] if len(df_resultset) > 0 else 0
 
# Avoid divide-by-zero by ensuring average_count is not zero
    average_count = (mdm_count + df_count) / 2 if (mdm_count + df_count) != 0 else 1  
 
# Calculate percentage difference
    percentage_difference = abs(mdm_count - df_count) / average_count * 100
 
    if mdm_count == 0:
       status = "Zero Records In MDM"
    elif df_count >= mdm_count:
       status = "GREEN"
    elif percentage_difference <= 10:
       status = "GREEN"
    elif percentage_difference <= 15:
      status = "AMBER"
    else:
      status = "RED"
 
    subject_detail = f"{status}: {name}"
 
    cursor.close()
    connection.close()
    return html_section, excel_files, subject_detail, status
 
# ---- Main Parallel Execution ----
main_tables = ["LegacyAcn", "Customer360Hierarchy", "Customer360Outlet", "Customer360Acn"]
 
all_html_sections = []
all_excel_files = []
subject_details = []
subject_status = "GREEN"
 
error_occurred = False
main_table_statuses = []
 
# Store actual resultsets for summary extraction
table_resultsets = {}
 
# --- FIX: Use a dict to map table name to result ---
results_by_name = {}
 
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    future_to_name = {executor.submit(process_table, tm): tm['name'] for tm in table_mappings}
    for future in concurrent.futures.as_completed(future_to_name):
        name = future_to_name[future]
        result = future.result()
        results_by_name[name] = result
 
for tm in table_mappings:
    name = tm['name']
    result = results_by_name.get(name)
    if result is None:
        error_occurred = True
        continue
    html_section, excel_files, subject_detail, status = result
    all_html_sections.append(html_section)
    all_excel_files.extend(excel_files)
    subject_details.append(subject_detail)
    if name in main_tables:
        main_table_statuses.append(status)
 
    # --- FIX: Extract actual resultsets for summary table for each table ---
    try:
        mdm_resultset = pd.read_html(StringIO(html_section))[0]
        df_resultset = pd.read_html(StringIO(html_section))[1] if "Record Count in Data Fabric" in html_section else pd.DataFrame()
    except Exception:
        mdm_resultset = pd.DataFrame()
        df_resultset = pd.DataFrame()
    table_resultsets[name] = (mdm_resultset, df_resultset, status)
 
# Determine overall status based only on main tables
if "RED" in main_table_statuses:
    subject_status = "RED"
elif "AMBER" in main_table_statuses:
    subject_status = "AMBER"
else:
    subject_status = "GREEN"
 
# ---- Send Combined Email ----
if error_occurred:
    print("[ERROR] Errors occurred during processing. Email will NOT be sent.")
else:
    table_names = [tm['name'] for tm in table_mappings]
    table_names_str = ', '.join(table_names)
 
    # 1. Professional subject
    SUBJECT = f"Reconciliation Report - {yesterday_dt.strftime(format3)}"
 
    # 2. Build summary table and variance summary for all tables
    summary_rows = []
    variance_summaries = []
    sl_no = 1
    VARIANCE_THRESHOLD = 5  # Only show in attention if variance > 5%
 
    for tm in table_mappings:
        name = tm['name']
        mdm_resultset, df_resultset, status = table_resultsets.get(name, (pd.DataFrame(), pd.DataFrame(), ""))
        mdm_full = mdm_24h = df_full = df_24h = 0
        if not mdm_resultset.empty and 'TYPE' in mdm_resultset.columns and 'RECORD_COUNT' in mdm_resultset.columns:
            if 'Full Table Count' in mdm_resultset['TYPE'].values:
                mdm_full = mdm_resultset.loc[mdm_resultset['TYPE'] == 'Full Table Count', 'RECORD_COUNT'].values[0]
            if 'Last 24 Hours Record Count' in mdm_resultset['TYPE'].values:
                mdm_24h = mdm_resultset.loc[mdm_resultset['TYPE'] == 'Last 24 Hours Record Count', 'RECORD_COUNT'].values[0]
        if not df_resultset.empty:
            if 'Total Number of records' in df_resultset.columns:
                df_full = df_resultset['Total Number of records'].values[0]
            if 'Total Number of records in last 24Hrs' in df_resultset.columns:
                df_24h = df_resultset['Total Number of records in last 24Hrs'].values[0]
 
        # Special handling for Del tables
        if name in ["Customer360OutletDel", "Customer360HierarchyDel"]:
            df_full = 0
            df_24h = del_table_df_counts.get(name, 0)
            # Fix: If both are zero, set GREEN
            if mdm_24h == 0 and df_24h == 0:
                sync_status = "GREEN"
            elif df_24h == 0:
                sync_status = "RED"
            elif df_24h < 10:
                sync_status = "YELLOW"
            else:
                sync_status = "GREEN"
            percent_var = ""
        else:
            try:
                mdm_24h_int = int(mdm_24h)
                df_24h_int = int(df_24h)
                if mdm_24h_int == 0 and df_24h_int == 0:
                    percent_var = 0
                elif mdm_24h_int == 0:
                    percent_var = 100
                else:
                    percent_var = round(abs(mdm_24h_int - df_24h_int) / mdm_24h_int * 100, 2)
            except Exception:
                percent_var = ""
 
            try:
                if mdm_24h_int == 0 and df_24h_int == 0:
                    sync_status = "GREEN"
                elif percent_var <= 5:
                    sync_status = "GREEN"
                elif percent_var <= 15:
                    sync_status = "YELLOW"
                else:
                    sync_status = "RED"
            except Exception:
                sync_status = ""
 
        summary_rows.append([
            sl_no, name, mdm_full, df_full, mdm_24h, df_24h, sync_status
        ])
 
        # Only include in variance summary if:
        # - Not a Del table
        # - NOT SYNC
        # - Variance above threshold and not 0.0%
        if (
            "Del" not in name
            and sync_status == "NOT SYNC"
            and isinstance(percent_var, (int, float))
            and percent_var > VARIANCE_THRESHOLD
        ):
            variance_summaries.append(
                f"<li><b>{name}</b>: {percent_var}% variance between MDM and Data Fabric 24hr counts (MDM: {mdm_24h}, DF: {df_24h})</li>"
            )
        sl_no += 1
 
    # --- Build summary table with row coloring except Sl.No and Table Name ---
    summary_heading_html = "<h2 style='color:#1a237e; text-align:center;'>Reconciliation Summary Table</h2>"
 
    summary_table_html = """
    <table border="1" style="border-collapse: collapse; width: 95%; margin: 0 auto; font-size: 14px;">
        <tr>
            <th>Sl.No</th>
            <th>Table Name</th>
            <th>MDM Count (Full Table Count)</th>
            <th>DF Count (Full Table Count)</th>
            <th>MDM Count (Last 24Hrs)</th>
            <th>DF Count (Last 24 Hrs)</th>
            <th>Last 24Hrs Sync Status</th>
        </tr>
    """
    for row in summary_rows:
        status = str(row[6]).upper()
        if status == "GREEN":
            bgcolor = "#c8e6c9"
            fontcolor = "#388e3c"
        elif status == "YELLOW":
            bgcolor = "#fff9c4"
            fontcolor = "#fbc02d"
        elif status == "RED":
            bgcolor = "#ffcdd2"
            fontcolor = "#d32f2f"
        elif status == "NA":
            bgcolor = "#eeeeee"
            fontcolor = "#757575"
        else:
            bgcolor = "#ffffff"
            fontcolor = "#222"
 
        summary_table_html += "<tr>"
        # First column (Sl.No) with no color
        summary_table_html += f"<td>{row[0]}</td>"
        # All other columns with color
        for col in row[1:]:
            summary_table_html += (
                f"<td bgcolor='{bgcolor}' style='background-color:{bgcolor}; color:{fontcolor}; font-weight:bold;'>{col}</td>"
            )
        summary_table_html += "</tr>"
 
    summary_table_html += "</table><br>"
 
    # --- OneDrive Link Section ---
    sharepoint_local_path = r"C:\Users\Z22461\The Coca-Cola Company\Data Fabric and ESP Support - DailyReconciliationReportDumps"
    onedrive_url = "file:///" + sharepoint_local_path.replace("\\", "/")  # For clickable file link in Outlook
 
    onedrive_html = f"""
    <div style="margin: 30px 0 10px 0; text-align:center;">
        <span style="font-size:16px; color:#1565c0; font-weight:bold;">&#128193; Excel Reports Location:</span><br>
        <a href="{onedrive_url}" style="font-size:15px; color:#0d47a1; text-decoration:underline;">
            {sharepoint_local_path}
        </a>
        <div style="font-size:13px; color:#555; margin-top:5px;">
            (All reconciliation Excel files for this run are available in the above OneDrive folder.)
        </div>
    </div>
    """
 
    # ---- Email body (no quick summary, no attachments) ----
    BODY_HTML = f"""
    <html>
    <head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            font-size: 15px;
            color: #222;
            margin: 0;
            padding: 0;
        }}
        h1 {{
            text-align: center;
            color: #1a237e;
            margin-bottom: 10px;
        }}
        .section-title {{
            color: #1565c0;
            margin-top: 30px;
            margin-bottom: 10px;
        }}
        table {{
            border-collapse: collapse;
            margin: 0 auto 30px auto;
            width: 90%;
        }}
        th, td {{
            border: 1px solid #888;
            padding: 8px 12px;
            text-align: center;
        }}
        th {{
            background-color: #f2f2f2;
            color: #0d47a1;
        }}
        ul {{
            margin: 0 0 0 20px;
            padding: 0;
        }}
        .signature {{
            color: #444;
            text-align: left;
            margin: 40px auto 0 auto;
            font-size: 14px;
            width: 90%;
            max-width: 900px;
        }}
    </style>
    </head>
    <body>
        <h1>NAOU Data Fabric Daily Reconciliation for {yesterday_dt.strftime(format3)}</h1>
        {summary_heading_html}
        {summary_table_html}
        {onedrive_html}
        <div class="signature">
            <br>
            Thanks,<br>
            <b>NAOU Middleware Data Fabric Support</b><br>
            cgdatafabricsupport@coca-cola.com
        </div>
    </body>
    </html>
    """
 
    msg = MIMEMultipart('related')
    msg['Subject'] = SUBJECT
    msg['From'] = SENDER
    msg['To'] = RECIPIENT
 
    msg_body = MIMEMultipart('alternative')
    textpart = MIMEText(BODY_HTML, 'html', CHARSET)
    msg_body.attach(textpart)
    msg.attach(msg_body)
 
    print("[DEBUG] Sending email without attachments (files are on OneDrive)...")
    try:
        response = client_email.send_email(
            Source=SENDER,
            Destination={'ToAddresses': [RECIPIENT]},
            Message={
                'Subject': {'Data': SUBJECT, 'Charset': CHARSET},
                'Body': {'Html': {'Data': BODY_HTML, 'Charset': CHARSET}}
            }
        )
        print(f"[INFO] Email sent! Message ID:", response['MessageId'])
    except ClientError as e:
        print("[ERROR] SES ClientError:", e.response['Error']['Message'])
    except Exception as e:
        print("[ERROR] SES Exception:", e)
 
print("Main table statuses:", main_table_statuses)
 
# ---- Print summary table to terminal ----
summary_df = pd.DataFrame(
    summary_rows,
    columns=[
        "Sl.No",
        "Table Name",
        "MDM Count (Full Table Count)",
        "DF Count (Full Table Count)",
        "MDM Count (Last 24Hrs)",
        "DF Count (Last 24 Hrs)",
        "Last 24Hrs Sync Status"
    ]
)
print("\n==== Reconciliation Summary Table ====")
print(summary_df.to_string(index=False))
print("======================================\n")
 
 
