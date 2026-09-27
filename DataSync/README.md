import pandas as pd
import xml.etree.ElementTree as ET
import snowflake.connector
import re
from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

XML_FILE = Path(
    r"C:\Users\061055\OneDrive - Freddie Mac"
    r"\Desktop\LQA_Request_File\EDS_document_1.xml"
)

FILE_NAME = "CompleteXMLFile_LQA_req.xml"

OUTPUT_FILE = XML_FILE.parent / "XML_Snowflake_Comparison.csv"
MISMATCH_FILE = XML_FILE.parent / "XML_Snowflake_Mismatches.csv"

SNOWFLAKE_CONFIG = {
    "account": "YOUR_ACCOUNT",
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "role": "YOUR_ROLE",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "ADIPROD",
    "schema": "CARRWCIADEV"
}


# ============================================================
# PATH VALIDATION
# ============================================================

print("\n==========================================")
print("PATH VALIDATION")
print("==========================================")
print("XML File:", XML_FILE)
print("XML Exists:", XML_FILE.exists())
print("Output Folder:", XML_FILE.parent)
print("Output Folder Exists:", XML_FILE.parent.exists())

if not XML_FILE.exists():
    raise FileNotFoundError(f"XML file not found: {XML_FILE}")


# ============================================================
# STEP 1 - READ XML AND CONVERT TO DATAFRAME
# ============================================================

print("\n==========================================")
print("STEP 1 - READ XML")
print("==========================================")

tree = ET.parse(XML_FILE)
root = tree.getroot()

xml_rows = []

for elem in root.iter():
    tag = elem.tag.split("}")[-1]

    # Capture leaf elements only.
    # This also keeps empty XML elements as None.
    if len(list(elem)) == 0:
        value = elem.text.strip() if elem.text and elem.text.strip() else None

        xml_rows.append({
            "XML_COLUMN": tag,
            "XML_VALUE": value
        })

xml_df = pd.DataFrame(xml_rows)

print("\nXML DataFrame:")
print(xml_df.head(20))
print("\nXML Shape:", xml_df.shape)
print("Unique XML Columns:", xml_df["XML_COLUMN"].nunique())


# ============================================================
# NORMALIZE COLUMN NAMES
# ============================================================

def normalize_column_name(column_name):
    if pd.isna(column_name):
        return None

    return re.sub(
        r"[^A-Za-z0-9]",
        "",
        str(column_name)
    ).upper()


xml_df["MAPPING_KEY"] = xml_df["XML_COLUMN"].apply(
    normalize_column_name
)

print("\nNormalized XML:")
print(
    xml_df[
        ["XML_COLUMN", "MAPPING_KEY", "XML_VALUE"]
    ].head(20)
)


# ============================================================
# STEP 2 - CONNECT TO SNOWFLAKE
# ============================================================

print("\n==========================================")
print("STEP 2 - CONNECT TO SNOWFLAKE")
print("==========================================")

conn = snowflake.connector.connect(
    account=SNOWFLAKE_CONFIG["account"],
    user=SNOWFLAKE_CONFIG["user"],
    password=SNOWFLAKE_CONFIG["password"],
    role=SNOWFLAKE_CONFIG["role"],
    warehouse=SNOWFLAKE_CONFIG["warehouse"],
    database=SNOWFLAKE_CONFIG["database"],
    schema=SNOWFLAKE_CONFIG["schema"]
)

print("Snowflake connection successful")


# ============================================================
# SNOWFLAKE SQL
# ============================================================

sql_query = f"""
WITH ALL_VALUES AS (
    SELECT
        'LQA_REQ_BORROWER_STG' AS TABLE_NAME,
        F.KEY::STRING AS COLUMN_NAME,
        F.VALUE::STRING AS VALUE
    FROM (
        SELECT *, OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ
        FROM CARRWCIADEV.LQA_REQ_BORROWER_STG
        WHERE FILENAME = '{FILE_NAME}'
    ) T,
    LATERAL FLATTEN(INPUT => T.OBJ) F

    UNION ALL

    SELECT
        'LQA_REQ_LOANRISK_ASSESSMENT_STG' AS TABLE_NAME,
        F.KEY::STRING AS COLUMN_NAME,
        F.VALUE::STRING AS VALUE
    FROM (
        SELECT *, OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ
        FROM CARRWCIADEV.LQA_REQ_LOANRISK_ASSESSMENT_STG
        WHERE FILENAME = '{FILE_NAME}'
    ) T,
    LATERAL FLATTEN(INPUT => T.OBJ) F

    UNION ALL

    SELECT
        'LQA_REQ_LOAN_STATE_STG' AS TABLE_NAME,
        F.KEY::STRING AS COLUMN_NAME,
        F.VALUE::STRING AS VALUE
    FROM (
        SELECT *, OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ
        FROM CARRWCIADEV.LQA_REQ_LOAN_STATE_STG
        WHERE FILENAME = '{FILE_NAME}'
    ) T,
    LATERAL FLATTEN(INPUT => T.OBJ) F

    UNION ALL

    SELECT
        'LQA_REQ_LOAN_STATE_ADD_STG' AS TABLE_NAME,
        F.KEY::STRING AS COLUMN_NAME,
        F.VALUE::STRING AS VALUE
    FROM (
        SELECT *, OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ
        FROM CARRWCIADEV.LQA_REQ_LOAN_STATE_ADD_STG
        WHERE FILENAME = '{FILE_NAME}'
    ) T,
    LATERAL FLATTEN(INPUT => T.OBJ) F

    UNION ALL

    SELECT
        'LQA_REQ_PROPERTY_STG' AS TABLE_NAME,
        F.KEY::STRING AS COLUMN_NAME,
        F.VALUE::STRING AS VALUE
    FROM (
        SELECT *, OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ
        FROM CARRWCIADEV.LQA_REQ_PROPERTY_STG
        WHERE FILENAME = '{FILE_NAME}'
    ) T,
    LATERAL FLATTEN(INPUT => T.OBJ) F

    UNION ALL

    SELECT
        'LQA_REQ_PROPERTY_APPRAISAL_STG' AS TABLE_NAME,
        F.KEY::STRING AS COLUMN_NAME,
        F.VALUE::STRING AS VALUE
    FROM (
        SELECT *, OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ
        FROM CARRWCIADEV.LQA_REQ_PROPERTY_APPRAISAL_STG
        WHERE FILENAME = '{FILE_NAME}'
    ) T,
    LATERAL FLATTEN(INPUT => T.OBJ) F

    UNION ALL

    SELECT
        'LQA_REQ_PARTYROLES_STG' AS TABLE_NAME,
        F.KEY::STRING AS COLUMN_NAME,
        F.VALUE::STRING AS VALUE
    FROM (
        SELECT *, OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ
        FROM CARRWCIADEV.LQA_REQ_PARTYROLES_STG
        WHERE FILENAME = '{FILE_NAME}'
    ) T,
    LATERAL FLATTEN(INPUT => T.OBJ) F

    UNION ALL

    SELECT
        'LQA_REQ_PREVIOUSEVALUATIONRESULTS_STG' AS TABLE_NAME,
        F.KEY::STRING AS COLUMN_NAME,
        F.VALUE::STRING AS VALUE
    FROM (
        SELECT *, OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ
        FROM CARRWCIADEV.LQA_REQ_PREVIOUSEVALUATIONRESULTS_STG
        WHERE FILENAME = '{FILE_NAME}'
    ) T,
    LATERAL FLATTEN(INPUT => T.OBJ) F

    UNION ALL

    SELECT
        'LQA_REQ_KEYS_STG' AS TABLE_NAME,
        F.KEY::STRING AS COLUMN_NAME,
        F.VALUE::STRING AS VALUE
    FROM (
        SELECT *, OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ
        FROM CARRWCIADEV.LQA_REQ_KEYS_STG
        WHERE FILENAME = '{FILE_NAME}'
    ) T,
    LATERAL FLATTEN(INPUT => T.OBJ) F
)
SELECT
    TABLE_NAME,
    COLUMN_NAME,
    VALUE
FROM ALL_VALUES
ORDER BY TABLE_NAME, COLUMN_NAME;
"""


# ============================================================
# EXECUTE SNOWFLAKE SQL
# ============================================================

cursor = conn.cursor()

try:
    cursor.execute(sql_query)
    rows = cursor.fetchall()

    column_names = [
        desc[0]
        for desc in cursor.description
    ]

    snowflake_df = pd.DataFrame(
        rows,
        columns=column_names
    )

finally:
    cursor.close()
    conn.close()

print("\nSnowflake DataFrame:")
print(snowflake_df.head(20))
print("\nSnowflake Shape:", snowflake_df.shape)


# ============================================================
# RENAME SNOWFLAKE COLUMNS
# ============================================================

snowflake_df = snowflake_df.rename(
    columns={
        "COLUMN_NAME": "SNOWFLAKE_COLUMN",
        "VALUE": "SNOWFLAKE_VALUE"
    }
)


# ============================================================
# REMOVE TECHNICAL DATABASE COLUMNS
# ============================================================

technical_columns = [
    "FILENAME"
    # "CREATED_DATE",
    # "UPDATED_DATE",
    # "LOAD_TIMESTAMP"
]

snowflake_df = snowflake_df[
    ~snowflake_df["SNOWFLAKE_COLUMN"]
    .fillna("")
    .str.upper()
    .isin(technical_columns)
].copy()


# ============================================================
# STEP 3 - XML / SNOWFLAKE COLUMN MAPPING
# ============================================================

print("\n==========================================")
print("STEP 3 - COLUMN MAPPING")
print("==========================================")

snowflake_df["MAPPING_KEY"] = (
    snowflake_df["SNOWFLAKE_COLUMN"]
    .apply(normalize_column_name)
)


# ============================================================
# OPTIONAL MANUAL MAPPING
# ============================================================
#
# Add mapping only when XML and DB names are actually different.
#
# XML:
# BorrowerFirstName
#
# DB:
# BORR_FIRST_NM
#
# Add:
# "BORROWERFIRSTNAME": "BORRFIRSTNM"
# ============================================================

manual_mapping = {
    # "BORROWERFIRSTNAME": "BORRFIRSTNM",
    # "LOANIDENTIFIER": "LOANID"
}

xml_df["MAPPING_KEY"] = (
    xml_df["MAPPING_KEY"]
    .replace(manual_mapping)
)


# ============================================================
# VALUE CLEANING
# ============================================================

def clean_value(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value.upper() in {
        "",
        "NULL",
        "NONE",
        "NAN"
    }:
        return None

    return value


xml_df["XML_VALUE"] = xml_df["XML_VALUE"].apply(
    clean_value
)

snowflake_df["SNOWFLAKE_VALUE"] = (
    snowflake_df["SNOWFLAKE_VALUE"]
    .apply(clean_value)
)


# ============================================================
# COMBINE DUPLICATE VALUES
# ============================================================

def combine_unique_values(series):
    values = []

    for value in series:
        if value is None:
            continue

        value = str(value)

        if value not in values:
            values.append(value)

    if not values:
        return None

    return " | ".join(values)


# ============================================================
# GROUP XML
# ============================================================

xml_grouped = (
    xml_df
    .groupby(
        "MAPPING_KEY",
        dropna=False
    )
    .agg({
        "XML_COLUMN": combine_unique_values,
        "XML_VALUE": combine_unique_values
    })
    .reset_index()
)

print("\nGrouped XML:")
print(xml_grouped.head(20))


# ============================================================
# GROUP SNOWFLAKE
# ============================================================

snowflake_grouped = (
    snowflake_df
    .groupby(
        ["TABLE_NAME", "MAPPING_KEY"],
        dropna=False
    )
    .agg({
        "SNOWFLAKE_COLUMN": combine_unique_values,
        "SNOWFLAKE_VALUE": combine_unique_values
    })
    .reset_index()
)

print("\nGrouped Snowflake:")
print(snowflake_grouped.head(20))


# ============================================================
# STEP 4 - COMPARE XML AND SNOWFLAKE
# ============================================================

print("\n==========================================")
print("STEP 4 - DATA COMPARISON")
print("==========================================")

comparison_df = pd.merge(
    snowflake_grouped,
    xml_grouped,
    on="MAPPING_KEY",
    how="outer"
)


# ============================================================
# COMPARISON LOGIC
# ============================================================

def compare_row(row):
    xml_column = row["XML_COLUMN"]
    db_column = row["SNOWFLAKE_COLUMN"]
    xml_value = clean_value(row["XML_VALUE"])
    db_value = clean_value(row["SNOWFLAKE_VALUE"])

    if pd.isna(xml_column):
        return "NOT_IN_XML_PRESENT_IN_DATABASE"

    if pd.isna(db_column):
        return "PRESENT_IN_XML_NOT_IN_DATABASE"

    if xml_value is None and db_value is None:
        return "MATCH"

    if xml_value is None and db_value is not None:
        return "VALUE_MISSING_IN_XML"

    if xml_value is not None and db_value is None:
        return "VALUE_MISSING_IN_DATABASE"

    if xml_value == db_value:
        return "MATCH"

    return "VALUE_MISMATCH"


comparison_df["STATUS"] = comparison_df.apply(
    compare_row,
    axis=1
)


# ============================================================
# ADD VALIDATION COLUMNS
# ============================================================

comparison_df["COLUMN_PRESENT_IN_XML"] = (
    comparison_df["XML_COLUMN"].notna()
)

comparison_df["COLUMN_PRESENT_IN_DATABASE"] = (
    comparison_df["SNOWFLAKE_COLUMN"].notna()
)

comparison_df["IS_MATCH"] = (
    comparison_df["STATUS"] == "MATCH"
)


# ============================================================
# FINAL COLUMN ORDER
# ============================================================

comparison_df = comparison_df[
    [
        "TABLE_NAME",
        "XML_COLUMN",
        "SNOWFLAKE_COLUMN",
        "XML_VALUE",
        "SNOWFLAKE_VALUE",
        "STATUS",
        "COLUMN_PRESENT_IN_XML",
        "COLUMN_PRESENT_IN_DATABASE",
        "IS_MATCH",
        "MAPPING_KEY"
    ]
]


# ============================================================
# SORT ISSUES FIRST
# ============================================================

status_order = {
    "VALUE_MISMATCH": 1,
    "VALUE_MISSING_IN_XML": 2,
    "VALUE_MISSING_IN_DATABASE": 3,
    "NOT_IN_XML_PRESENT_IN_DATABASE": 4,
    "PRESENT_IN_XML_NOT_IN_DATABASE": 5,
    "MATCH": 6
}

comparison_df["SORT_ORDER"] = (
    comparison_df["STATUS"]
    .map(status_order)
)

comparison_df = (
    comparison_df
    .sort_values(
        [
            "SORT_ORDER",
            "TABLE_NAME",
            "SNOWFLAKE_COLUMN",
            "XML_COLUMN"
        ],
        na_position="last"
    )
    .drop(columns=["SORT_ORDER"])
    .reset_index(drop=True)
)


# ============================================================
# STEP 5 - EXPORT RESULT TO CSV
# ============================================================

print("\n==========================================")
print("STEP 5 - EXPORT CSV")
print("==========================================")

comparison_df.to_csv(
    OUTPUT_FILE,
    index=False
)

issues_df = comparison_df[
    comparison_df["STATUS"] != "MATCH"
].copy()

issues_df.to_csv(
    MISMATCH_FILE,
    index=False
)


# ============================================================
# SUMMARY
# ============================================================

summary_df = (
    comparison_df["STATUS"]
    .value_counts()
    .reset_index()
)

summary_df.columns = [
    "STATUS",
    "COUNT"
]

print("\n==========================================")
print("VALIDATION SUMMARY")
print("==========================================")
print(summary_df.to_string(index=False))


# ============================================================
# DISPLAY MISMATCHES
# ============================================================

print("\n==========================================")
print("MISMATCH / MISSING RECORDS")
print("==========================================")

if issues_df.empty:
    print("No mismatches found.")
else:
    print(
        issues_df[
            [
                "TABLE_NAME",
                "XML_COLUMN",
                "SNOWFLAKE_COLUMN",
                "XML_VALUE",
                "SNOWFLAKE_VALUE",
                "STATUS"
            ]
        ].to_string(index=False)
    )


# ============================================================
# FINAL RESULT
# ============================================================

print("\n==========================================")
print("PROCESS COMPLETED")
print("==========================================")
print("Total Records :", len(comparison_df))
print(
    "Total Matches :",
    (comparison_df["STATUS"] == "MATCH").sum()
)
print("Total Issues  :", len(issues_df))
print("\nComparison CSV:")
print(OUTPUT_FILE)
print("\nMismatch CSV:")
print(MISMATCH_FILE)
