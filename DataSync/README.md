import pandas as pd
import xml.etree.ElementTree as ET
import snowflake.connector
import re
from collections import defaultdict


# ============================================================
# CONFIGURATION
# ============================================================

XML_FILE = r"C:\Users\061055\OneDrive - Freddie Mac\Desktop\LQA_Request_File\EDS_document_1.xml"

FILE_NAME = "CompleteXMLFile_LQA_req.xml"

OUTPUT_FILE = r"C:\Users\061055\OneDrive - Freddie Mac\Desktop\LQA_Request_File\XML_Snowflake_Comparison.csv"


SNOWFLAKE_CONFIG = {
    "account": "YOUR_ACCOUNT",
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "role": "YOUR_ROLE",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "ADIPROD",
    "schema": "CARRWCIADEV"
}


TABLES = [
    "LQA_REQ_BORROWER_STG",
    "LQA_REQ_LOANRISK_ASSESSMENT_STG",
    "LQA_REQ_LOAN_STATE_STG",
    "LQA_REQ_LOAN_STATE_ADD_STG",
    "LQA_REQ_PROPERTY_STG",
    "LQA_REQ_PROPERTY_APPRAISAL_STG",
    "LQA_REQ_PARTYROLES_STG",
    "LQA_REQ_PREVIOUSEVALUATIONRESULTS_STG",
    "LQA_REQ_KEYS_STG"
]


# ============================================================
# STEP 1
# READ XML AND CONVERT TO DATAFRAME
# ============================================================

def read_xml_to_dataframe(xml_file):

    tree = ET.parse(xml_file)
    root = tree.getroot()

    xml_data = []

    for elem in root.iter():

        # Remove XML namespace
        tag = elem.tag.split("}")[-1]

        if elem.text and elem.text.strip():

            value = elem.text.strip()

            xml_data.append({
                "XML_COLUMN": tag,
                "XML_VALUE": value
            })

    xml_df = pd.DataFrame(xml_data)

    print("\n========================================")
    print("XML DATA")
    print("========================================")

    print(xml_df.head())
    print("XML rows:", len(xml_df))
    print("Unique XML columns:", xml_df["XML_COLUMN"].nunique())

    return xml_df


xml_df = read_xml_to_dataframe(XML_FILE)


# ============================================================
# HELPER FUNCTION
# NORMALIZE COLUMN NAMES
#
# Examples:
# LoanIdentifier
# LOAN_IDENTIFIER
# loan-identifier
#
# All become:
# LOANIDENTIFIER
# ============================================================

def normalize_column_name(column_name):

    if pd.isna(column_name):
        return None

    return re.sub(
        r"[^A-Za-z0-9]",
        "",
        str(column_name)
    ).upper()


xml_df["NORMALIZED_COLUMN"] = (
    xml_df["XML_COLUMN"]
    .apply(normalize_column_name)
)


# ============================================================
# OPTIONAL MANUAL MAPPING
#
# Use this only when XML field and DB column names are
# completely different.
#
# Example:
# XML: PROPERTYADDRESS
# DB : SUBJECT_PROPERTY_ADDRESS
# ============================================================

manual_mapping = {

    # "XML_FIELD_NAME": "SNOWFLAKE_COLUMN_NAME",

    # Example:
    # "LoanIdentifier": "LOAN_IDENTIFIER",
    # "BorrowerFirstName": "BORROWER_FIRST_NAME"
}


manual_normalized_mapping = {
    normalize_column_name(k):
    normalize_column_name(v)
    for k, v in manual_mapping.items()
}


def apply_manual_mapping(normalized_name):

    return manual_normalized_mapping.get(
        normalized_name,
        normalized_name
    )


xml_df["MAPPING_KEY"] = (
    xml_df["NORMALIZED_COLUMN"]
    .apply(apply_manual_mapping)
)


# ============================================================
# STEP 2
# CONNECT TO SNOWFLAKE AND READ TABLE DATA
# ============================================================

conn = snowflake.connector.connect(
    account=SNOWFLAKE_CONFIG["account"],
    user=SNOWFLAKE_CONFIG["user"],
    password=SNOWFLAKE_CONFIG["password"],
    role=SNOWFLAKE_CONFIG["role"],
    warehouse=SNOWFLAKE_CONFIG["warehouse"],
    database=SNOWFLAKE_CONFIG["database"],
    schema=SNOWFLAKE_CONFIG["schema"]
)


snowflake_records = []


for table in TABLES:

    print(f"\nReading Snowflake table: {table}")

    query = f"""
        SELECT *
        FROM {SNOWFLAKE_CONFIG["database"]}.
             {SNOWFLAKE_CONFIG["schema"]}.
             {table}
        WHERE FILENAME = %s
    """

    cur = conn.cursor()

    try:

        cur.execute(query, (FILE_NAME,))

        rows = cur.fetchall()

        column_names = [
            desc[0]
            for desc in cur.description
        ]

        table_df = pd.DataFrame(
            rows,
            columns=column_names
        )

        print(
            f"{table} -> {len(table_df)} rows"
        )

        # --------------------------------------------
        # Convert every DB column/value into long format
        # --------------------------------------------

        for _, row in table_df.iterrows():

            for column in column_names:

                value = row[column]

                snowflake_records.append({
                    "TABLE_NAME": table,
                    "SNOWFLAKE_COLUMN": column,
                    "SNOWFLAKE_VALUE": value
                })

    except Exception as e:

        print(
            f"{table} -> ERROR: {e}"
        )

    finally:
        cur.close()


conn.close()


snowflake_df = pd.DataFrame(
    snowflake_records
)


print("\n========================================")
print("SNOWFLAKE DATA")
print("========================================")

print(snowflake_df.head())

print(
    "Snowflake rows:",
    len(snowflake_df)
)


# ============================================================
# REMOVE TECHNICAL COLUMNS IF REQUIRED
# ============================================================

technical_columns = [
    "FILENAME"
    # Add additional fields here if needed:
    # "CREATED_DATE",
    # "UPDATED_DATE",
    # "LOAD_TIMESTAMP"
]


snowflake_df = snowflake_df[
    ~snowflake_df["SNOWFLAKE_COLUMN"]
    .str.upper()
    .isin(technical_columns)
].copy()


# ============================================================
# NORMALIZE SNOWFLAKE COLUMN NAMES
# ============================================================

snowflake_df["NORMALIZED_COLUMN"] = (
    snowflake_df["SNOWFLAKE_COLUMN"]
    .apply(normalize_column_name)
)

snowflake_df["MAPPING_KEY"] = (
    snowflake_df["NORMALIZED_COLUMN"]
)


# ============================================================
# CLEAN VALUES BEFORE COMPARISON
# ============================================================

def clean_value(value):

    if pd.isna(value):
        return None

    value = str(value).strip()

    # Treat these as NULL
    if value.upper() in [
        "",
        "NULL",
        "NONE",
        "NAN"
    ]:
        return None

    return value


xml_df["XML_VALUE_CLEAN"] = (
    xml_df["XML_VALUE"]
    .apply(clean_value)
)

snowflake_df["SNOWFLAKE_VALUE_CLEAN"] = (
    snowflake_df["SNOWFLAKE_VALUE"]
    .apply(clean_value)
)


# ============================================================
# HANDLE DUPLICATE XML VALUES
#
# Example:
# BorrowerName = John
# BorrowerName = Mary
#
# Instead of losing one value:
# John | Mary
# ============================================================

def combine_values(series):

    values = []

    for value in series:

        if value is not None:

            value = str(value)

            if value not in values:
                values.append(value)

    return " | ".join(values)


xml_grouped = (
    xml_df
    .groupby(
        ["MAPPING_KEY"],
        dropna=False
    )
    .agg({
        "XML_COLUMN": combine_values,
        "XML_VALUE_CLEAN": combine_values
    })
    .reset_index()
)


# ============================================================
# GROUP SNOWFLAKE VALUES
#
# Keep TABLE_NAME because same field can exist in
# different staging tables.
# ============================================================

snowflake_grouped = (
    snowflake_df
    .groupby(
        [
            "TABLE_NAME",
            "MAPPING_KEY"
        ],
        dropna=False
    )
    .agg({
        "SNOWFLAKE_COLUMN": combine_values,
        "SNOWFLAKE_VALUE_CLEAN": combine_values
    })
    .reset_index()
)


# ============================================================
# STEP 3
# XML <-> SNOWFLAKE COLUMN MAPPING
#
# OUTER JOIN IS IMPORTANT
#
# It keeps:
# 1. Matching fields
# 2. XML-only fields
# 3. Database-only fields
# ============================================================

comparison_df = pd.merge(
    snowflake_grouped,
    xml_grouped,
    on="MAPPING_KEY",
    how="outer"
)


# ============================================================
# STEP 4
# IDENTIFY MATCH / MISMATCH / MISSING FIELDS
# ============================================================

def determine_status(row):

    xml_column = row.get("XML_COLUMN")
    db_column = row.get("SNOWFLAKE_COLUMN")

    xml_value = clean_value(
        row.get("XML_VALUE_CLEAN")
    )

    db_value = clean_value(
        row.get("SNOWFLAKE_VALUE_CLEAN")
    )

    # ------------------------------------
    # Exists only in Snowflake
    # ------------------------------------

    if pd.isna(xml_column):

        return "NOT_IN_XML_PRESENT_IN_DATABASE"

    # ------------------------------------
    # Exists only in XML
    # ------------------------------------

    if pd.isna(db_column):

        return "PRESENT_IN_XML_NOT_IN_DATABASE"

    # ------------------------------------
    # Both NULL
    # ------------------------------------

    if xml_value is None and db_value is None:

        return "MATCH"

    # ------------------------------------
    # XML NULL but DB contains value
    # ------------------------------------

    if xml_value is None and db_value is not None:

        return "VALUE_MISSING_IN_XML"

    # ------------------------------------
    # DB NULL but XML contains value
    # ------------------------------------

    if db_value is None and xml_value is not None:

        return "VALUE_MISSING_IN_DATABASE"

    # ------------------------------------
    # Compare values
    # ------------------------------------

    if str(xml_value).strip() == str(db_value).strip():

        return "MATCH"

    return "VALUE_MISMATCH"


comparison_df["STATUS"] = (
    comparison_df.apply(
        determine_status,
        axis=1
    )
)


# ============================================================
# ADD USEFUL INDICATORS
# ============================================================

comparison_df["COLUMN_FOUND_IN_XML"] = (
    comparison_df["XML_COLUMN"]
    .notna()
)

comparison_df["COLUMN_FOUND_IN_DATABASE"] = (
    comparison_df["SNOWFLAKE_COLUMN"]
    .notna()
)


# ============================================================
# REORDER FINAL REPORT
# ============================================================

comparison_df = comparison_df[
    [
        "TABLE_NAME",
        "XML_COLUMN",
        "SNOWFLAKE_COLUMN",
        "XML_VALUE_CLEAN",
        "SNOWFLAKE_VALUE_CLEAN",
        "STATUS",
        "COLUMN_FOUND_IN_XML",
        "COLUMN_FOUND_IN_DATABASE",
        "MAPPING_KEY"
    ]
]


comparison_df = comparison_df.rename(
    columns={
        "XML_VALUE_CLEAN": "XML_VALUE",
        "SNOWFLAKE_VALUE_CLEAN": "SNOWFLAKE_VALUE"
    }
)


# ============================================================
# SORT RESULTS
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
            "SNOWFLAKE_COLUMN"
        ],
        na_position="last"
    )
    .drop(
        columns=["SORT_ORDER"]
    )
)


# ============================================================
# STEP 5
# EXPORT RESULT TO CSV
# ============================================================

comparison_df.to_csv(
    OUTPUT_FILE,
    index=False
)


# ============================================================
# SUMMARY
# ============================================================

print("\n========================================")
print("VALIDATION SUMMARY")
print("========================================")

summary = (
    comparison_df["STATUS"]
    .value_counts()
    .reset_index()
)

summary.columns = [
    "STATUS",
    "COUNT"
]

print(summary.to_string(index=False))


print("\n========================================")
print("MISMATCH / MISSING DATA")
print("========================================")

issues_df = comparison_df[
    comparison_df["STATUS"] != "MATCH"
]

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


print("\n========================================")
print("CSV CREATED")
print("========================================")

print(OUTPUT_FILE)
