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
    {
        "name": "Customer360HierMap",
        "df_table": "ccna-fab-prod-Customer360HierMap",
        "mdm_table": "V_DF_CUSTOMER360_HIER_MAP",
        "mdm_filter": "Last_Update_Date",
        "df_key_condition_expression": "hierarchySource = :hierarchySource AND lastUpdateDate BETWEEN :sdt AND :edt",
        "df_sort_key_values": [
            {"0": "WAREHOUSE"},
            {"1": "NABDB"},
            {"2": "Mngd BY"},
            {"3": "Concessionaire"},
            {"4": "FSOP"}
        ],
        "df_index": "BYLASTUPDATEDATE",
        "partition_key": ":hierarchySource",
        "merge_key": "uniqueId",
        "mdm_columns": "UNIQUE_ID as uniqueId, LAST_UPDATE_DATE as lastUpdateDate, FSOP_ID as fsopId, FSOP_NAME as fsopName, CBS_ACCOUNT_NO as cbsAccountNo, CBS_ACCOUNT_NAME as cbsAccountName, FSOP_MAPPING_LEVEL as fsopMappingLevel, CBS_MAPPING_LEVEL as cbsMappingLevel, NABDB_PE_ID as nabdbPeId, NABDB_PE_NAME as nabdbPeName, HIERARCHY_SOURCE as hierarchySource, MAPPING_DECISION as mappingDecision, ORPHAN_OUTLET_FLAG as orphanOutletFlag, FSOP_BP_ID as fsopBpId, LINE_OF_BUSINESS as lineOfBusiness"
    },
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
    },
    {
        "name": "Customer360PositionMarketId",
        "df_table": "ccna-fab-prod-Customer360PositionMarketId",
        "mdm_table": "V_DF_CUSTOMER360_POSITION_MARKETID",
        "mdm_filter": "Last_Update_Date",
        "df_key_condition_expression": "status = :status AND lastUpdateDate BETWEEN :sdt AND :edt",
        "df_sort_key_values": [
            {"0": "A"},
            {"1": "I"}
        ],
        "df_index": "BYLASTUPDATEDATE",
        "partition_key": ":status",
        "merge_key": "uniqueId",
        "mdm_columns": "UNIQUE_ID as uniqueId, LAST_UPDATE_DATE as lastUpdateDate, SALES_PSTN_ID as salesPstnId, LEGACY_MARKET_ID as legacyMarketId, STATUS as status"
    },
    {
        "name": "Customer360SalesPosition",
        "df_table": "ccna-fab-prod-Customer360SalesPosition",
        "mdm_table": "V_DF_CUSTOMER360_SALES_POSITION",
        "mdm_filter": "Last_Update_Date",
        "df_key_condition_expression": "revCenterId = :revCenterId AND lastUpdateDate BETWEEN :sdt AND :edt",
        "df_sort_key_values": [
            {"0": "RC1"},
            {"1": "RC10"},
            {"2": "RC11"},
            {"3": "RC12"},
            {"4": "RC13"},
            {"5": "RC14"},
            {"6": "RC15"},
            {"7": "RC17"},
            {"8": "RC18"},
            {"9": "RC2"},
            {"10": "RC20"},
            {"11": "RC24"},
            {"12": "RC25"},
            {"13": "RC26"},
            {"14": "RC3"},
            {"15": "RC4"},
            {"16": "RC5"},
            {"17": "RC6"},
            {"18": "RC7"},
            {"19": "RC8"},
            {"20": "RC9"}
        ],
        "df_index": "BYLASTUPDATEDATE",
        "partition_key": ":revCenterId",
        "merge_key": "uniqueId",
        "mdm_columns": "UNIQUE_ID as uniqueId, LAST_UPDATE_DATE as lastUpdateDate, SALES_POSITION_ID as salesPositionId, SALES_POSITION_NAME as salesPositionName, SALES_PERSON_ID as salesPersonId, SALES_PERSON_NAME as salesPersonName, SALES_PERSON_EMAIL as salesPersonEmail, SALES_PERSON_PHONE as salesPersonPhone, REV_CENTER_ID as revCenterId, REV_CENTER_NAME as revCenterIdName, P08_COST_CENTER as p08CostCenter, P08_COST_CENTER_DESC as p08CostCenterDesc, S4_COST_CENTER as s4CostCenter, PARENT_SALES_PSTN_ID as parentSalesPstnId, PARENT_POSITION_NAME as parentPostionName"
    }
]
# The `table_mappings` list now contains all the required table configurations exactly as defined in the data provided.
# ---- Email Config ----
session_email = boto3.Session(profile_name='CCNA_INTSRVC_NonProd_INT_AppAdmin', region_name='us-west-2')
client_email = session_email.client('ses')
SENDER = "no-reply@coca-cola.com"
#RECIPIENT = "vvempati@coca-cola.com"
RECIPIENT = "cgdatafabricsupport@coca-cola.com"
CHARSET = "UTF-8"
 
# ---- Timeframe ----t
eastern = pytz.timezone("America/New_York")
yesterday_dt = datetime.now(eastern).date() - timedelta(days=1)
naive_start = datetime.combine(yesterday_dt, time.min)
naive_end = datetime.combine(yesterday_dt, time.max)
yesterday_start_dt = eastern.localize(naive_start)
yesterday_end_dt = eastern.localize(naive_end)
format1 = "%Y-%m-%d %H:%M:%S"
format2 = "%Y-%m-%dT%H:%M:%S.%f"
format3 = "%Y-%m-%d"
 
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
                    df.iloc[start:end].to_excel(writer, sheet_name=sheet_name, index=False)
        files.append(fname)
    return files
 
# ---- Per-table Processing Function ----
def process_table(table_mapping):
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
                        items = [item for item in items if item.get('c360DelFlag') == 'A'] #and not str(item.get('uniqueId', '')).upper().endswith('UNMATCHED')]
                    df_record.extend(items)
                    while 'LastEvaluatedKey' in response:
                        response = datafabric_table.query(
                            IndexName=table_mapping['df_index'],
                            KeyConditionExpression=table_mapping['df_key_condition_expression'],
                            ExpressionAttributeValues=expr_attr,
                            Limit=75000,
                            ExclusiveStartKey=response['LastEvaluatedKey']
                        )
                        items = response.get('Items', [])
                        if name in ["Customer360Hierarchy", "Customer360Outlet"]:
                            items = [item for item in items if item.get('c360DelFlag') == 'A']#and not str(item.get('uniqueId', '')).upper().endswith('UNMATCHED')]
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
    sharepoint_local_path = r"C:\Users\Z20419\OneDrive - The Coca-Cola Company\Data Fabric and ESP Support - General\DailyReconciliationReportDumps"
    for excel_file in excel_files:
        dest_path = os.path.join(sharepoint_local_path, os.path.basename(excel_file))
        try:
            shutil.copy2(excel_file, dest_path)
            print(f"[INFO] Copied {excel_file} to SharePoint folder: {dest_path}")
        except Exception as e:
            print(f"[ERROR] Failed to copy {excel_file} to SharePoint: {e}")
 
    # ---- Prepare HTML for Email ----
    mdm_html1 = pd.DataFrame(data=mdm_resultset).to_html()
    if name not in ["Customer360OutletDel", "Customer360HierarchyDel"]:
        df_html1 = pd.DataFrame(data=df_resultset).to_html()
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
    elif percentage_difference <= 25:
       status = "GREEN"
    elif percentage_difference <= 50:
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

# This block must be present!
with concurrent.futures.ThreadPoolExecutor(max_workers=128) as executor:
    futures = [executor.submit(process_table, tm) for tm in table_mappings]
    results = [f.result() for f in concurrent.futures.as_completed(futures)]

for result, tm in zip(results, table_mappings):
    if result is None:
        error_occurred = True
        continue
    html_section, excel_files, subject_detail, status = result
    all_html_sections.append(html_section)
    all_excel_files.extend(excel_files)
    subject_details.append(subject_detail)
    # Only consider main tables for overall status
    if tm['name'] in main_tables:
        main_table_statuses.append(status)

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

    SUBJECT = f"{subject_status}: Reconciliation Report For Tables: {table_names_str} {yesterday_dt.strftime(format3)}"
    BODY_HTML = f"""
    <html>
    <head>
    <style>
        body {{
            font-family: Arial, sans-serif;
        }}
        h1 {{
            text-align: center;
            color: #1a237e;
        }}
        h2 {{
            text-align: left;
            color: #1565c0;
        }}
        h3 {{
            text-align: left;
            color: #ef6c00;
        }}
        table {{
            border-collapse: collapse;
            margin: 0 auto;
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
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        tr:hover {{
            background-color: #e3f2fd;
        }}
        p {{
            color: brown;
            text-align: left;
            margin-left: 10%;
        }}
    </style>
    </head>
    <body>
    <h1>{SUBJECT}</h1>
    {''.join(all_html_sections)}
    <p>Thanks,<br>
    Data Fabric Support Team<br>
    cgdatafabricsupport@coca-cola.com<br>
    </p>
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
 
    # Attach Excel files one by one, checking total size
    attached_files = []
    max_email_size = 10485760  # 10 MB
    for excel_file in all_excel_files:
        with open(excel_file, 'rb') as f:
            attachment = MIMEApplication(f.read())
        attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(excel_file))
        msg.attach(attachment)
        if len(msg.as_string()) > max_email_size:
            msg.get_payload().pop()  # Remove last attachment
            break
        attached_files.append(excel_file)
 
    email_body_size = len(msg.as_string())
    print(f"[DEBUG] Total email size (bytes): {email_body_size}")
 
    if attached_files:
        try:
            response = client_email.send_raw_email(
                Source=SENDER,
                Destinations=[RECIPIENT],
                RawMessage={'Data': msg.as_string()}
            )
            print(f"[INFO] Email sent! Message ID:", response['MessageId'])
        except ClientError as e:
            print("[ERROR] SES ClientError:", e.response['Error']['Message'])
        except Exception as e:
            print("[ERROR] SES Exception:", e)
    else:
        print("[WARN] Email body size exceeds the limit. Sending email without attachment.")
        try:
            response = client_email.send_email(
                Source=SENDER,
                Destination={'ToAddresses': [RECIPIENT]},
                Message={
                    'Subject': {'Data': SUBJECT, 'Charset': CHARSET},
                    'Body': {'Html': {'Data': BODY_HTML, 'Charset': CHARSET}}
                }
            )
            print(f"[INFO] Email sent without attachment! Message ID:", response['MessageId'])
        except ClientError as e:
            print("[ERROR] SES ClientError:", e.response['Error']['Message'])
        except Exception as e:
            print("[ERROR] SES Exception:", e)

print("Main table statuses:", main_table_statuses)
