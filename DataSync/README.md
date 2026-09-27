import pandas as pd
import xml.etree.ElementTree as ET
import snowflake.connector
import re


# ============================================================
# CONFIGURATION
# ============================================================

XML_FILE = (
    r"C:\Users\061055\OneDrive - Freddie Mac"
    r"\Desktop\LQA_Request_File\EDS_document_1.xml"
)

FILE_NAME = "CompleteXMLFile_LQA_req.xml"

OUTPUT_FILE = (
    r"C:\Users\061055\OneDrive - Freddie Mac"
    r"\Desktop\LQA_Request_File"
    r"\XML_Snowflake_Comparison.csv"
)


# ============================================================
# STEP 1
# READ XML AND CONVERT XML TO DATAFRAME
# ============================================================

print("\n==========================================")
print("STEP 1 - READING XML")
print("==========================================")

tree = ET.parse(XML_FILE)

root = tree.getroot()


xml_rows = []


for elem in root.iter():

    # ------------------------------------------
    # Remove namespace
    #
    # Example:
    # {http://abc.com}LoanIdentifier
    #
    # becomes:
    # LoanIdentifier
    # ------------------------------------------

    tag = elem.tag.split("}")[-1]

    # ------------------------------------------
    # Read only elements containing values
    # ------------------------------------------

    if elem.text and elem.text.strip():

        value = elem.text.strip()

        xml_rows.append(
            {
                "XML_COLUMN": tag,
                "XML_VALUE": value
            }
        )


# Convert XML data to DataFrame

xml_df = pd.DataFrame(xml_rows)


print("\nXML DataFrame:")
print(xml_df.head(20))

print("\nXML shape:")
print(xml_df.shape)

print("\nUnique XML columns:")
print(xml_df["XML_COLUMN"].nunique())


# ============================================================
# NORMALIZATION FUNCTION
#
# Example:
#
# LoanIdentifier
# LOAN_IDENTIFIER
# loan-identifier
#
# all become:
#
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


# Create normalized XML column

xml_df["MAPPING_KEY"] = (
    xml_df["XML_COLUMN"]
    .apply(normalize_column_name)
)


print("\nNormalized XML:")
print(
    xml_df[
        [
            "XML_COLUMN",
            "MAPPING_KEY",
            "XML_VALUE"
        ]
    ].head(20)
)


# ============================================================
# STEP 2
# CONNECT SNOWFLAKE
# ============================================================

print("\n==========================================")
print("STEP 2 - CONNECTING TO SNOWFLAKE")
print("==========================================")


conn = snowflake.connector.connect(

    account="YOUR_ACCOUNT",

    user="YOUR_USER",

    password="YOUR_PASSWORD",

    role="YOUR_ROLE",

    warehouse="YOUR_WAREHOUSE",

    database="ADIPROD",

    schema="CARRWCIADEV"
)


print("Snowflake connection successful")


# ============================================================
# YOUR SNOWFLAKE SQL
# ============================================================

sql_query = f"""

WITH ALL_VALUES AS (

SELECT
    'LQA_REQ_BORROWER_STG' AS TABLE_NAME,
    F.KEY::STRING AS COLUMN_NAME,
    F.VALUE::STRING AS VALUE
FROM (

    SELECT
        *,
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ

    FROM CARRWCIADEV.LQA_REQ_BORROWER_STG

    WHERE FILENAME = '{FILE_NAME}'

) T,

LATERAL FLATTEN(
    INPUT => T.OBJ
) F


UNION ALL


SELECT
    'LQA_REQ_LOANRISK_ASSESSMENT_STG' AS TABLE_NAME,
    F.KEY::STRING AS COLUMN_NAME,
    F.VALUE::STRING AS VALUE
FROM (

    SELECT
        *,
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ

    FROM CARRWCIADEV.LQA_REQ_LOANRISK_ASSESSMENT_STG

    WHERE FILENAME = '{FILE_NAME}'

) T,

LATERAL FLATTEN(
    INPUT => T.OBJ
) F


UNION ALL


SELECT
    'LQA_REQ_LOAN_STATE_STG' AS TABLE_NAME,
    F.KEY::STRING AS COLUMN_NAME,
    F.VALUE::STRING AS VALUE
FROM (

    SELECT
        *,
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ

    FROM CARRWCIADEV.LQA_REQ_LOAN_STATE_STG

    WHERE FILENAME = '{FILE_NAME}'

) T,

LATERAL FLATTEN(
    INPUT => T.OBJ
) F


UNION ALL


SELECT
    'LQA_REQ_LOAN_STATE_ADD_STG' AS TABLE_NAME,
    F.KEY::STRING AS COLUMN_NAME,
    F.VALUE::STRING AS VALUE
FROM (

    SELECT
        *,
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ

    FROM CARRWCIADEV.LQA_REQ_LOAN_STATE_ADD_STG

    WHERE FILENAME = '{FILE_NAME}'

) T,

LATERAL FLATTEN(
    INPUT => T.OBJ
) F


UNION ALL


SELECT
    'LQA_REQ_PROPERTY_STG' AS TABLE_NAME,
    F.KEY::STRING AS COLUMN_NAME,
    F.VALUE::STRING AS VALUE
FROM (

    SELECT
        *,
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ

    FROM CARRWCIADEV.LQA_REQ_PROPERTY_STG

    WHERE FILENAME = '{FILE_NAME}'

) T,

LATERAL FLATTEN(
    INPUT => T.OBJ
) F


UNION ALL


SELECT
    'LQA_REQ_PROPERTY_APPRAISAL_STG' AS TABLE_NAME,
    F.KEY::STRING AS COLUMN_NAME,
    F.VALUE::STRING AS VALUE
FROM (

    SELECT
        *,
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ

    FROM CARRWCIADEV.LQA_REQ_PROPERTY_APPRAISAL_STG

    WHERE FILENAME = '{FILE_NAME}'

) T,

LATERAL FLATTEN(
    INPUT => T.OBJ
) F


UNION ALL


SELECT
    'LQA_REQ_PARTYROLES_STG' AS TABLE_NAME,
    F.KEY::STRING AS COLUMN_NAME,
    F.VALUE::STRING AS VALUE
FROM (

    SELECT
        *,
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ

    FROM CARRWCIADEV.LQA_REQ_PARTYROLES_STG

    WHERE FILENAME = '{FILE_NAME}'

) T,

LATERAL FLATTEN(
    INPUT => T.OBJ
) F


UNION ALL


SELECT
    'LQA_REQ_PREVIOUSEVALUATIONRESULTS_STG' AS TABLE_NAME,
    F.KEY::STRING AS COLUMN_NAME,
    F.VALUE::STRING AS VALUE
FROM (

    SELECT
        *,
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ

    FROM CARRWCIADEV.LQA_REQ_PREVIOUSEVALUATIONRESULTS_STG

    WHERE FILENAME = '{FILE_NAME}'

) T,

LATERAL FLATTEN(
    INPUT => T.OBJ
) F


UNION ALL


SELECT
    'LQA_REQ_KEYS_STG' AS TABLE_NAME,
    F.KEY::STRING AS COLUMN_NAME,
    F.VALUE::STRING AS VALUE
FROM (

    SELECT
        *,
        OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ

    FROM CARRWCIADEV.LQA_REQ_KEYS_STG

    WHERE FILENAME = '{FILE_NAME}'

) T,

LATERAL FLATTEN(
    INPUT => T.OBJ
) F

)


SELECT

    TABLE_NAME,

    COLUMN_NAME,

    VALUE

FROM ALL_VALUES

ORDER BY
    TABLE_NAME,
    COLUMN_NAME

"""


# ============================================================
# EXECUTE SQL
# ============================================================

cursor = conn.cursor()


try:

    cursor.execute(sql_query)

    rows = cursor.fetchall()

    snowflake_columns = [
        desc[0]
        for desc in cursor.description
    ]

    snowflake_df = pd.DataFrame(
        rows,
        columns=snowflake_columns
    )


finally:

    cursor.close()

    conn.close()


print("\nSnowflake DataFrame:")
print(snowflake_df.head(20))


print("\nSnowflake shape:")
print(snowflake_df.shape)


# ============================================================
# RENAME SNOWFLAKE COLUMNS
# ============================================================

snowflake_df = snowflake_df.rename(

    columns={

        "COLUMN_NAME":
            "SNOWFLAKE_COLUMN",

        "VALUE":
            "SNOWFLAKE_VALUE"

    }

)


# ============================================================
# REMOVE TECHNICAL DATABASE COLUMNS
#
# Add any columns here which should not be compared with XML.
# ============================================================

technical_columns = [

    "FILENAME"

    # Example:
    # "CREATED_DATE",
    # "UPDATED_DATE",
    # "LOAD_TIMESTAMP",
    # "INSERT_TIMESTAMP"

]


snowflake_df = snowflake_df[

    ~snowflake_df[
        "SNOWFLAKE_COLUMN"
    ]
    .str.upper()
    .isin(technical_columns)

].copy()


# ============================================================
# STEP 3
# MAP XML COLUMNS TO SNOWFLAKE COLUMNS
# ============================================================

print("\n==========================================")
print("STEP 3 - COLUMN MAPPING")
print("==========================================")


snowflake_df["MAPPING_KEY"] = (

    snowflake_df[
        "SNOWFLAKE_COLUMN"
    ]

    .apply(
        normalize_column_name
    )

)


# ============================================================
# OPTIONAL MANUAL MAPPING
#
# Use this only if XML column name and DB column name are
# completely different.
#
# Example:
#
# XML:
# BorrowerFirstName
#
# Snowflake:
# BORR_FIRST_NM
# ============================================================

manual_mapping = {

    # "BORROWERFIRSTNAME":
    # "BORRFIRSTNM",

    # "LOANIDENTIFIER":
    # "LOANID"

}


# Apply mapping to XML

xml_df["MAPPING_KEY"] = (

    xml_df["MAPPING_KEY"]
    .replace(
        manual_mapping
    )

)


# ============================================================
# CLEAN VALUES
# ============================================================

def clean_value(value):

    if pd.isna(value):
        return None

    value = str(value).strip()

    if value.upper() in [

        "",

        "NULL",

        "NONE",

        "NAN"

    ]:

        return None

    return value


xml_df["XML_VALUE"] = (

    xml_df["XML_VALUE"]
    .apply(clean_value)

)


snowflake_df["SNOWFLAKE_VALUE"] = (

    snowflake_df[
        "SNOWFLAKE_VALUE"
    ]

    .apply(clean_value)

)


# ============================================================
# HANDLE DUPLICATE XML VALUES
#
# Example:
#
# BorrowerName = John
# BorrowerName = Mary
#
# becomes:
#
# John | Mary
# ============================================================

def combine_unique_values(series):

    values = []

    for value in series:

        if value is None:
            continue

        value = str(value)

        if value not in values:
            values.append(value)

    if len(values) == 0:
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

        "XML_COLUMN":
            combine_unique_values,

        "XML_VALUE":
            combine_unique_values

    })

    .reset_index()

)


print("\nXML grouped:")
print(xml_grouped.head(20))


# ============================================================
# GROUP SNOWFLAKE
#
# TABLE_NAME is retained so we know exactly which table
# contains a mismatch.
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

        "SNOWFLAKE_COLUMN":
            combine_unique_values,

        "SNOWFLAKE_VALUE":
            combine_unique_values

    })

    .reset_index()

)


print("\nSnowflake grouped:")
print(
    snowflake_grouped.head(20)
)


# ============================================================
# STEP 4
# COMPARE XML AND SNOWFLAKE DATA
# ============================================================

print("\n==========================================")
print("STEP 4 - COMPARISON")
print("==========================================")


# OUTER JOIN IS IMPORTANT
#
# It captures:
#
# 1. XML + Snowflake
# 2. XML only
# 3. Snowflake only

comparison_df = pd.merge(

    snowflake_grouped,

    xml_grouped,

    on="MAPPING_KEY",

    how="outer"

)


# ============================================================
# DETERMINE STATUS
# ============================================================

def compare_row(row):

    xml_column = row[
        "XML_COLUMN"
    ]

    snowflake_column = row[
        "SNOWFLAKE_COLUMN"
    ]

    xml_value = row[
        "XML_VALUE"
    ]

    snowflake_value = row[
        "SNOWFLAKE_VALUE"
    ]


    # --------------------------------------------------------
    # Database column exists but XML column does not
    # --------------------------------------------------------

    if pd.isna(xml_column):

        return (
            "NOT_IN_XML_PRESENT_IN_DATABASE"
        )


    # --------------------------------------------------------
    # XML column exists but database column does not
    # --------------------------------------------------------

    if pd.isna(snowflake_column):

        return (
            "PRESENT_IN_XML_NOT_IN_DATABASE"
        )


    # --------------------------------------------------------
    # Both values are NULL
    # --------------------------------------------------------

    if (
        xml_value is None
        and snowflake_value is None
    ):

        return "MATCH"


    # --------------------------------------------------------
    # XML value missing
    # --------------------------------------------------------

    if (
        xml_value is None
        and snowflake_value is not None
    ):

        return "VALUE_MISSING_IN_XML"


    # --------------------------------------------------------
    # Database value missing
    # --------------------------------------------------------

    if (
        snowflake_value is None
        and xml_value is not None
    ):

        return "VALUE_MISSING_IN_DATABASE"


    # --------------------------------------------------------
    # Exact comparison
    # --------------------------------------------------------

    if (
        str(xml_value).strip()
        ==
        str(snowflake_value).strip()
    ):

        return "MATCH"


    # --------------------------------------------------------
    # Otherwise mismatch
    # --------------------------------------------------------

    return "VALUE_MISMATCH"


comparison_df["STATUS"] = (

    comparison_df.apply(
        compare_row,
        axis=1
    )

)


# ============================================================
# ADD PRESENCE FLAGS
# ============================================================

comparison_df[
    "COLUMN_PRESENT_IN_XML"
] = (

    comparison_df[
        "XML_COLUMN"
    ].notna()

)


comparison_df[
    "COLUMN_PRESENT_IN_DATABASE"
] = (

    comparison_df[
        "SNOWFLAKE_COLUMN"
    ].notna()

)


# ============================================================
# ADD MATCH FLAG
# ============================================================

comparison_df[
    "IS_MATCH"
] = (

    comparison_df[
        "STATUS"
    ]
    == "MATCH"

)


# ============================================================
# REORDER FINAL OUTPUT
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
# SORT MISMATCHES FIRST
# ============================================================

status_order = {

    "VALUE_MISMATCH":
        1,

    "VALUE_MISSING_IN_XML":
        2,

    "VALUE_MISSING_IN_DATABASE":
        3,

    "NOT_IN_XML_PRESENT_IN_DATABASE":
        4,

    "PRESENT_IN_XML_NOT_IN_DATABASE":
        5,

    "MATCH":
        6

}


comparison_df[
    "SORT_ORDER"
] = (

    comparison_df[
        "STATUS"
    ]

    .map(
        status_order
    )

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

    .drop(
        columns=[
            "SORT_ORDER"
        ]
    )

)


# ============================================================
# STEP 5
# EXPORT FINAL RESULT TO CSV
# ============================================================

print("\n==========================================")
print("STEP 5 - EXPORT CSV")
print("==========================================")


comparison_df.to_csv(

    OUTPUT_FILE,

    index=False

)


print(
    f"\nCSV created successfully:\n{OUTPUT_FILE}"
)


# ============================================================
# PRINT SUMMARY
# ============================================================

print("\n==========================================")
print("VALIDATION SUMMARY")
print("==========================================")


summary_df = (

    comparison_df[
        "STATUS"
    ]

    .value_counts()

    .reset_index()

)


summary_df.columns = [

    "STATUS",

    "COUNT"

]


print(
    summary_df.to_string(
        index=False
    )
)


# ============================================================
# PRINT ONLY ISSUES
# ============================================================

print("\n==========================================")
print("MISMATCH / MISSING RECORDS")
print("==========================================")


issues_df = (

    comparison_df[

        comparison_df[
            "STATUS"
        ]

        != "MATCH"

    ]

)


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

    ]

    .to_string(
        index=False
    )

)


# ============================================================
# OPTIONAL:
# CREATE SEPARATE MISMATCH CSV
# ============================================================

MISMATCH_FILE = (

    r"C:\Users\061055\OneDrive - Freddie Mac"
    r"\Desktop\LQA_Request_File"
    r"\XML_Snowflake_Mismatches.csv"

)


issues_df.to_csv(

    MISMATCH_FILE,

    index=False

)


print(
    f"\nMismatch CSV created:\n{MISMATCH_FILE}"
)
