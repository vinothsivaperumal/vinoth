import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import snowflake.connector


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_FOLDER = Path(
    r"C:\Users\061055\OneDrive - Freddie Mac"
    r"\Desktop\LQA_Request_File"
)

OUTPUT_FOLDER = INPUT_FOLDER / "validation_output"

MAPPING_FILE = (
    INPUT_FOLDER
    / "xml_snowflake_column_mapping(1).json"
)

# Read every XML file from INPUT_FOLDER.
# Change to "EDS_*.xml" if only EDS files should be processed.
FILE_PATTERN = "*.xml"

CONTINUE_ON_ERROR = True

SNOWFLAKE_CONFIG = {
    "account": "YOUR_ACCOUNT",
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "role": "YOUR_ROLE",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "ADIPROD",
    "schema": "CARRWCIADEV"
}

# Use only when local XML filename differs from Snowflake FILENAME.
SNOWFLAKE_FILE_OVERRIDES = {
    # "EDS_document_1.xml": "CompleteXMLFile_LQA_req.xml",
    # "EDS_document_2.xml": "CompleteXMLFile_LQA_req_2.xml"
}

TECHNICAL_COLUMNS = {
    "FILENAME"
}

STATUS_ORDER = {
    "VALUE_MISMATCH": 1,
    "VALUE_MISSING_IN_XML": 2,
    "VALUE_MISSING_IN_DATABASE": 3,
    "NOT_IN_XML_PRESENT_IN_DATABASE": 4,
    "PRESENT_IN_XML_NOT_IN_DATABASE": 5,
    "NOT_PRESENT_IN_XML_OR_DATABASE": 6,
    "MATCH": 7
}


# ============================================================
# COMMON FUNCTIONS
# ============================================================

def normalize_name(value):
    if pd.isna(value):
        return None

    return re.sub(
        r"[^A-Za-z0-9]",
        "",
        str(value)
    ).upper()


def clean_value(value):
    if pd.isna(value):
        return None

    value = str(value).strip()

    if value.upper() in {"", "NULL", "NONE", "NAN"}:
        return None

    return value


def combine_unique_values(series):
    values = []

    for value in series:
        value = clean_value(value)

        if value is None:
            continue

        if value not in values:
            values.append(value)

    if not values:
        return None

    return " | ".join(sorted(values))


def validate_identifier(value):
    value = str(value).strip()

    if not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_$]*",
        value
    ):
        raise ValueError(
            f"Invalid Snowflake identifier: {value}"
        )

    return value


# ============================================================
# LOAD JSON MAPPING CONFIGURATION
# ============================================================

def load_mapping_config(mapping_file):
    if not mapping_file.exists():
        raise FileNotFoundError(
            f"Mapping file not found: {mapping_file}"
        )

    with open(
        mapping_file,
        "r",
        encoding="utf-8"
    ) as file:
        config = json.load(file)

    if "mappings" not in config:
        raise ValueError(
            "JSON must contain a 'mappings' array."
        )

    mapping_df = pd.DataFrame(
        config["mappings"]
    )

    required_columns = {
        "TABLE_NAME",
        "COLUMN_NAME",
        "XML_COLUMN_NAME"
    }

    missing_columns = (
        required_columns
        - set(mapping_df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Mapping JSON missing required fields: "
            f"{missing_columns}"
        )

    if mapping_df.empty:
        raise ValueError(
            "Mapping JSON contains no mappings."
        )

    mapping_df = mapping_df[
        [
            "TABLE_NAME",
            "COLUMN_NAME",
            "XML_COLUMN_NAME"
        ]
    ].copy()

    mapping_df["TABLE_NAME"] = (
        mapping_df["TABLE_NAME"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    mapping_df["COLUMN_NAME"] = (
        mapping_df["COLUMN_NAME"]
        .astype(str)
        .str.strip()
    )

    mapping_df["XML_COLUMN_NAME"] = (
        mapping_df["XML_COLUMN_NAME"]
        .astype(str)
        .str.strip()
    )

    mapping_df["DB_COLUMN_KEY"] = (
        mapping_df["COLUMN_NAME"]
        .apply(normalize_name)
    )

    mapping_df["XML_COLUMN_KEY"] = (
        mapping_df["XML_COLUMN_NAME"]
        .apply(normalize_name)
    )

    # If the normalized XML and DB names differ,
    # this is effectively a manual mapping supplied by JSON.
    mapping_df["MAPPING_TYPE"] = mapping_df.apply(
        lambda row: (
            "DIRECT"
            if row["DB_COLUMN_KEY"]
            == row["XML_COLUMN_KEY"]
            else "CONFIG_MAPPING"
        ),
        axis=1
    )

    duplicate_mask = mapping_df.duplicated(
        [
            "TABLE_NAME",
            "DB_COLUMN_KEY"
        ],
        keep=False
    )

    if duplicate_mask.any():
        duplicates = mapping_df.loc[
            duplicate_mask,
            [
                "TABLE_NAME",
                "COLUMN_NAME",
                "XML_COLUMN_NAME"
            ]
        ]

        raise ValueError(
            "Duplicate TABLE_NAME + COLUMN_NAME "
            "mapping found:\n"
            + duplicates.to_string(index=False)
        )

    mapping_df.insert(
        0,
        "MAPPING_ID",
        range(1, len(mapping_df) + 1)
    )

    return mapping_df


# ============================================================
# READ ALL XML FILES
# ============================================================

def get_xml_files():
    if not INPUT_FOLDER.exists():
        raise FileNotFoundError(
            f"Input folder not found: {INPUT_FOLDER}"
        )

    files = sorted(
        [
            file
            for file in INPUT_FOLDER.glob(FILE_PATTERN)
            if file.is_file()
        ],
        key=lambda file: file.name.lower()
    )

    return files


# ============================================================
# READ ONE XML FILE
# ============================================================

def read_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    rows = []

    for elem in root.iter():
        # Leaf elements only.
        if len(list(elem)) != 0:
            continue

        column_name = elem.tag.split("}")[-1]

        value = (
            elem.text.strip()
            if elem.text and elem.text.strip()
            else None
        )

        rows.append({
            "XML_COLUMN": column_name,
            "XML_COLUMN_KEY": normalize_name(column_name),
            "XML_VALUE": clean_value(value)
        })

    return pd.DataFrame(
        rows,
        columns=[
            "XML_COLUMN",
            "XML_COLUMN_KEY",
            "XML_VALUE"
        ]
    )


# ============================================================
# SNOWFLAKE CONNECTION
# ============================================================

def connect_snowflake():
    return snowflake.connector.connect(
        account=SNOWFLAKE_CONFIG["account"],
        user=SNOWFLAKE_CONFIG["user"],
        password=SNOWFLAKE_CONFIG["password"],
        role=SNOWFLAKE_CONFIG["role"],
        warehouse=SNOWFLAKE_CONFIG["warehouse"],
        database=SNOWFLAKE_CONFIG["database"],
        schema=SNOWFLAKE_CONFIG["schema"]
    )


# ============================================================
# BUILD VALIDATION SQL FROM JSON CONFIG
# ============================================================

def build_validation_query(mapping_df):
    database = validate_identifier(
        SNOWFLAKE_CONFIG["database"]
    )

    schema = validate_identifier(
        SNOWFLAKE_CONFIG["schema"]
    )

    tables = sorted(
        mapping_df["TABLE_NAME"]
        .dropna()
        .unique()
        .tolist()
    )

    if not tables:
        raise ValueError(
            "No Snowflake tables found in mapping config."
        )

    queries = []

    for table in tables:
        table = validate_identifier(table)

        query = f"""
SELECT
    '{table}' AS TABLE_NAME,
    F.KEY::STRING AS COLUMN_NAME,
    F.VALUE::STRING AS VALUE
FROM (
    SELECT *, OBJECT_CONSTRUCT_KEEP_NULL(*) AS OBJ
    FROM {database}.{schema}.{table}
    WHERE FILENAME = %s
) T,
LATERAL FLATTEN(INPUT => T.OBJ) F
""".strip()

        queries.append(query)

    union_query = "\n\nUNION ALL\n\n".join(queries)

    final_query = f"""
WITH ALL_VALUES AS (
{union_query}
)
SELECT
    TABLE_NAME,
    COLUMN_NAME,
    VALUE
FROM ALL_VALUES
ORDER BY TABLE_NAME, COLUMN_NAME
""".strip()

    return final_query, len(tables)


# ============================================================
# GET SNOWFLAKE FILENAME
# ============================================================

def get_snowflake_filename(xml_file):
    return SNOWFLAKE_FILE_OVERRIDES.get(
        xml_file.name,
        xml_file.name
    )


# ============================================================
# RUN SNOWFLAKE VALIDATION
# ============================================================

def run_validation_query(
    conn,
    query,
    parameter_count,
    snowflake_filename
):
    parameters = tuple(
        snowflake_filename
        for _ in range(parameter_count)
    )

    cursor = conn.cursor()

    try:
        cursor.execute(
            query,
            parameters
        )

        rows = cursor.fetchall()

        columns = [
            description[0]
            for description in cursor.description
        ]

        return pd.DataFrame(
            rows,
            columns=columns
        )

    finally:
        cursor.close()


# ============================================================
# PREPARE SNOWFLAKE DATAFRAME
# ============================================================

def prepare_snowflake_dataframe(df):
    required_columns = {
        "TABLE_NAME",
        "COLUMN_NAME",
        "VALUE"
    }

    missing_columns = (
        required_columns
        - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            "Snowflake query must return "
            "TABLE_NAME, COLUMN_NAME and VALUE. "
            f"Missing: {missing_columns}"
        )

    df = df.copy()

    df["TABLE_NAME"] = (
        df["TABLE_NAME"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df = df[
        ~df["COLUMN_NAME"]
        .fillna("")
        .str.upper()
        .isin(TECHNICAL_COLUMNS)
    ].copy()

    df["DB_COLUMN_KEY"] = (
        df["COLUMN_NAME"]
        .apply(normalize_name)
    )

    df["SNOWFLAKE_VALUE"] = (
        df["VALUE"]
        .apply(clean_value)
    )

    return df[
        [
            "TABLE_NAME",
            "COLUMN_NAME",
            "DB_COLUMN_KEY",
            "SNOWFLAKE_VALUE"
        ]
    ]


# ============================================================
# GROUP XML VALUES
# ============================================================

def group_xml(xml_df):
    if xml_df.empty:
        return pd.DataFrame(
            columns=[
                "XML_COLUMN_KEY",
                "ACTUAL_XML_COLUMN",
                "XML_VALUE",
                "XML_OCCURRENCE_COUNT",
                "XML_FIELD_PRESENT"
            ]
        )

    grouped = (
        xml_df
        .groupby(
            "XML_COLUMN_KEY",
            dropna=False
        )
        .agg(
            ACTUAL_XML_COLUMN=(
                "XML_COLUMN",
                combine_unique_values
            ),
            XML_VALUE=(
                "XML_VALUE",
                combine_unique_values
            ),
            XML_OCCURRENCE_COUNT=(
                "XML_COLUMN",
                "size"
            )
        )
        .reset_index()
    )

    grouped["XML_FIELD_PRESENT"] = True

    return grouped


# ============================================================
# GROUP SNOWFLAKE VALUES
# ============================================================

def group_snowflake(db_df):
    if db_df.empty:
        return pd.DataFrame(
            columns=[
                "TABLE_NAME",
                "DB_COLUMN_KEY",
                "ACTUAL_DB_COLUMN",
                "SNOWFLAKE_VALUE",
                "DB_OCCURRENCE_COUNT",
                "DB_COLUMN_PRESENT"
            ]
        )

    grouped = (
        db_df
        .groupby(
            [
                "TABLE_NAME",
                "DB_COLUMN_KEY"
            ],
            dropna=False
        )
        .agg(
            ACTUAL_DB_COLUMN=(
                "COLUMN_NAME",
                combine_unique_values
            ),
            SNOWFLAKE_VALUE=(
                "SNOWFLAKE_VALUE",
                combine_unique_values
            ),
            DB_OCCURRENCE_COUNT=(
                "COLUMN_NAME",
                "size"
            )
        )
        .reset_index()
    )

    grouped["DB_COLUMN_PRESENT"] = True

    return grouped


# ============================================================
# COMPARISON STATUS
# ============================================================

def determine_status(row):
    xml_present = bool(
        row["XML_FIELD_PRESENT"]
    )

    db_present = bool(
        row["DB_COLUMN_PRESENT"]
    )

    xml_value = clean_value(
        row["XML_VALUE"]
    )

    db_value = clean_value(
        row["SNOWFLAKE_VALUE"]
    )

    if not xml_present and db_present:
        return "NOT_IN_XML_PRESENT_IN_DATABASE"

    if xml_present and not db_present:
        return "PRESENT_IN_XML_NOT_IN_DATABASE"

    if not xml_present and not db_present:
        return "NOT_PRESENT_IN_XML_OR_DATABASE"

    if xml_value is None and db_value is None:
        return "MATCH"

    if xml_value is None:
        return "VALUE_MISSING_IN_XML"

    if db_value is None:
        return "VALUE_MISSING_IN_DATABASE"

    if xml_value == db_value:
        return "MATCH"

    return "VALUE_MISMATCH"


# ============================================================
# COMPARE USING JSON CONFIG
# ============================================================

def compare_using_mapping(
    xml_df,
    db_df,
    mapping_df,
    xml_file,
    snowflake_filename
):
    xml_grouped = group_xml(xml_df)
    db_grouped = group_snowflake(db_df)

    # Mapping JSON is the master list.
    comparison = mapping_df.copy()

    # Add Snowflake data using TABLE + COLUMN.
    comparison = comparison.merge(
        db_grouped,
        on=[
            "TABLE_NAME",
            "DB_COLUMN_KEY"
        ],
        how="left"
    )

    # Add XML data using configured XML_COLUMN_NAME.
    comparison = comparison.merge(
        xml_grouped,
        on="XML_COLUMN_KEY",
        how="left"
    )

    comparison["XML_FIELD_PRESENT"] = (
        comparison["XML_FIELD_PRESENT"]
        .fillna(False)
        .astype(bool)
    )

    comparison["DB_COLUMN_PRESENT"] = (
        comparison["DB_COLUMN_PRESENT"]
        .fillna(False)
        .astype(bool)
    )

    comparison["XML_VALUE"] = (
        comparison["XML_VALUE"]
        .apply(clean_value)
    )

    comparison["SNOWFLAKE_VALUE"] = (
        comparison["SNOWFLAKE_VALUE"]
        .apply(clean_value)
    )

    comparison["STATUS"] = (
        comparison.apply(
            determine_status,
            axis=1
        )
    )

    comparison["IS_MATCH"] = (
        comparison["STATUS"] == "MATCH"
    )

    comparison.insert(
        0,
        "XML_FILE",
        xml_file.name
    )

    comparison.insert(
        1,
        "SNOWFLAKE_FILE",
        snowflake_filename
    )

    comparison["SORT_ORDER"] = (
        comparison["STATUS"]
        .map(STATUS_ORDER)
    )

    comparison = (
        comparison
        .sort_values(
            [
                "SORT_ORDER",
                "TABLE_NAME",
                "COLUMN_NAME"
            ],
            na_position="last"
        )
        .drop(
            columns=["SORT_ORDER"]
        )
        .reset_index(drop=True)
    )

    return comparison[
        [
            "XML_FILE",
            "SNOWFLAKE_FILE",
            "MAPPING_ID",
            "MAPPING_TYPE",
            "TABLE_NAME",
            "COLUMN_NAME",
            "XML_COLUMN_NAME",
            "ACTUAL_DB_COLUMN",
            "ACTUAL_XML_COLUMN",
            "XML_VALUE",
            "SNOWFLAKE_VALUE",
            "XML_OCCURRENCE_COUNT",
            "DB_OCCURRENCE_COUNT",
            "XML_FIELD_PRESENT",
            "DB_COLUMN_PRESENT",
            "STATUS",
            "IS_MATCH"
        ]
    ]


# ============================================================
# FIND XML FIELDS NOT IN CONFIG
# ============================================================

def find_unmapped_xml(
    xml_df,
    mapping_df
):
    mapped_keys = set(
        mapping_df["XML_COLUMN_KEY"]
        .dropna()
    )

    unmapped = xml_df[
        ~xml_df["XML_COLUMN_KEY"]
        .isin(mapped_keys)
    ].copy()

    return unmapped


# ============================================================
# FIND DATABASE FIELDS NOT IN CONFIG
# ============================================================

def find_unmapped_database(
    db_df,
    mapping_df
):
    configured = (
        mapping_df[
            [
                "TABLE_NAME",
                "DB_COLUMN_KEY"
            ]
        ]
        .drop_duplicates()
    )

    result = db_df.merge(
        configured,
        on=[
            "TABLE_NAME",
            "DB_COLUMN_KEY"
        ],
        how="left",
        indicator=True
    )

    return (
        result[
            result["_merge"] == "left_only"
        ]
        .drop(columns=["_merge"])
        .reset_index(drop=True)
    )


# ============================================================
# CREATE FILE SUMMARY
# ============================================================

def create_file_summary(
    xml_file,
    snowflake_filename,
    mapping_df,
    comparison_df,
    unmapped_xml_df,
    unmapped_db_df
):
    counts = (
        comparison_df["STATUS"]
        .value_counts()
        .to_dict()
    )

    match_count = counts.get(
        "MATCH",
        0
    )

    validation_issues = (
        len(comparison_df)
        - match_count
    )

    total_issues = (
        validation_issues
        + len(unmapped_xml_df)
        + len(unmapped_db_df)
    )

    return {
        "XML_FILE":
            xml_file.name,

        "SNOWFLAKE_FILE":
            snowflake_filename,

        "TOTAL_CONFIG_MAPPINGS":
            len(mapping_df),

        "DIRECT_MAPPINGS":
            (
                mapping_df["MAPPING_TYPE"]
                == "DIRECT"
            ).sum(),

        "CONFIG_MAPPINGS":
            (
                mapping_df["MAPPING_TYPE"]
                == "CONFIG_MAPPING"
            ).sum(),

        "TOTAL_COMPARED":
            len(comparison_df),

        "MATCH":
            match_count,

        "VALUE_MISMATCH":
            counts.get(
                "VALUE_MISMATCH",
                0
            ),

        "VALUE_MISSING_IN_XML":
            counts.get(
                "VALUE_MISSING_IN_XML",
                0
            ),

        "VALUE_MISSING_IN_DATABASE":
            counts.get(
                "VALUE_MISSING_IN_DATABASE",
                0
            ),

        "NOT_IN_XML_PRESENT_IN_DATABASE":
            counts.get(
                "NOT_IN_XML_PRESENT_IN_DATABASE",
                0
            ),

        "PRESENT_IN_XML_NOT_IN_DATABASE":
            counts.get(
                "PRESENT_IN_XML_NOT_IN_DATABASE",
                0
            ),

        "NOT_PRESENT_IN_XML_OR_DATABASE":
            counts.get(
                "NOT_PRESENT_IN_XML_OR_DATABASE",
                0
            ),

        "UNMAPPED_XML_FIELDS":
            len(unmapped_xml_df),

        "UNMAPPED_DATABASE_FIELDS":
            len(unmapped_db_df),

        "TOTAL_ISSUES":
            total_issues,

        "RESULT":
            (
                "PASS"
                if total_issues == 0
                else "ISSUES_FOUND"
            ),

        "ERROR_MESSAGE":
            ""
    }


# ============================================================
# ERROR SUMMARY
# ============================================================

def create_error_summary(
    xml_file,
    error
):
    return {
        "XML_FILE":
            xml_file.name,

        "SNOWFLAKE_FILE":
            get_snowflake_filename(
                xml_file
            ),

        "TOTAL_CONFIG_MAPPINGS": 0,
        "DIRECT_MAPPINGS": 0,
        "CONFIG_MAPPINGS": 0,
        "TOTAL_COMPARED": 0,
        "MATCH": 0,
        "VALUE_MISMATCH": 0,
        "VALUE_MISSING_IN_XML": 0,
        "VALUE_MISSING_IN_DATABASE": 0,
        "NOT_IN_XML_PRESENT_IN_DATABASE": 0,
        "PRESENT_IN_XML_NOT_IN_DATABASE": 0,
        "NOT_PRESENT_IN_XML_OR_DATABASE": 0,
        "UNMAPPED_XML_FIELDS": 0,
        "UNMAPPED_DATABASE_FIELDS": 0,
        "TOTAL_ISSUES": 0,
        "RESULT": "ERROR",
        "ERROR_MESSAGE": str(error)
    }


# ============================================================
# SAVE FILE REPORTS
# ============================================================

def save_file_reports(
    xml_file,
    comparison_df,
    unmapped_xml_df,
    unmapped_db_df
):
    safe_name = re.sub(
        r"[^A-Za-z0-9_-]",
        "_",
        xml_file.stem
    )

    comparison_file = (
        OUTPUT_FOLDER
        / f"{safe_name}_Comparison.csv"
    )

    mismatch_file = (
        OUTPUT_FOLDER
        / f"{safe_name}_Mismatches.csv"
    )

    unmapped_xml_file = (
        OUTPUT_FOLDER
        / f"{safe_name}_Unmapped_XML.csv"
    )

    unmapped_db_file = (
        OUTPUT_FOLDER
        / f"{safe_name}_Unmapped_Database.csv"
    )

    comparison_df.to_csv(
        comparison_file,
        index=False
    )

    issues_df = comparison_df[
        comparison_df["STATUS"] != "MATCH"
    ].copy()

    issues_df.to_csv(
        mismatch_file,
        index=False
    )

    unmapped_xml_df.to_csv(
        unmapped_xml_file,
        index=False
    )

    unmapped_db_df.to_csv(
        unmapped_db_file,
        index=False
    )

    return issues_df


# ============================================================
# PROCESS ONE XML FILE
# ============================================================

def process_xml_file(
    conn,
    xml_file,
    mapping_df,
    validation_query,
    parameter_count
):
    snowflake_filename = (
        get_snowflake_filename(
            xml_file
        )
    )

    print("\n" + "=" * 70)
    print("XML File       :", xml_file.name)
    print("Snowflake File :", snowflake_filename)
    print("=" * 70)

    xml_df = read_xml(
        xml_file
    )

    snowflake_raw_df = (
        run_validation_query(
            conn,
            validation_query,
            parameter_count,
            snowflake_filename
        )
    )

    db_df = (
        prepare_snowflake_dataframe(
            snowflake_raw_df
        )
    )

    comparison_df = (
        compare_using_mapping(
            xml_df,
            db_df,
            mapping_df,
            xml_file,
            snowflake_filename
        )
    )

    unmapped_xml_df = (
        find_unmapped_xml(
            xml_df,
            mapping_df
        )
    )

    unmapped_db_df = (
        find_unmapped_database(
            db_df,
            mapping_df
        )
    )

    issues_df = save_file_reports(
        xml_file,
        comparison_df,
        unmapped_xml_df,
        unmapped_db_df
    )

    summary = create_file_summary(
        xml_file,
        snowflake_filename,
        mapping_df,
        comparison_df,
        unmapped_xml_df,
        unmapped_db_df
    )

    print("XML Rows       :", len(xml_df))
    print("Database Rows  :", len(db_df))
    print("Compared       :", summary["TOTAL_COMPARED"])
    print("Matched        :", summary["MATCH"])
    print("Issues         :", summary["TOTAL_ISSUES"])
    print("Result         :", summary["RESULT"])

    return {
        "comparison": comparison_df,
        "issues": issues_df,
        "unmapped_xml": unmapped_xml_df,
        "unmapped_db": unmapped_db_df,
        "summary": summary
    }


# ============================================================
# MAIN PROCESS
# ============================================================

def main():
    OUTPUT_FOLDER.mkdir(
        parents=True,
        exist_ok=True
    )

    # Load JSON only once.
    mapping_df = load_mapping_config(
        MAPPING_FILE
    )

    print("\nMapping Config :", MAPPING_FILE)
    print("Total Mappings :", len(mapping_df))
    print(
        "Config Mappings:",
        (
            mapping_df["MAPPING_TYPE"]
            == "CONFIG_MAPPING"
        ).sum()
    )

    # Build same validation SQL once.
    validation_query, parameter_count = (
        build_validation_query(
            mapping_df
        )
    )

    xml_files = get_xml_files()

    if not xml_files:
        print(
            f"No XML files found in {INPUT_FOLDER}"
        )
        return

    print("\nXML Files Found:", len(xml_files))

    for xml_file in xml_files:
        print(" -", xml_file.name)

    summary_rows = []
    all_comparisons = []
    all_issues = []
    all_unmapped_xml = []
    all_unmapped_db = []

    conn = None

    try:
        # One Snowflake connection for all files.
        conn = connect_snowflake()

        print(
            "\nSnowflake connection successful"
        )

        for xml_file in xml_files:
            try:
                result = process_xml_file(
                    conn,
                    xml_file,
                    mapping_df,
                    validation_query,
                    parameter_count
                )

                summary_rows.append(
                    result["summary"]
                )

                if not result["comparison"].empty:
                    all_comparisons.append(
                        result["comparison"]
                    )

                if not result["issues"].empty:
                    all_issues.append(
                        result["issues"]
                    )

                if not result["unmapped_xml"].empty:
                    temp = (
                        result["unmapped_xml"]
                        .copy()
                    )

                    temp.insert(
                        0,
                        "XML_FILE",
                        xml_file.name
                    )

                    all_unmapped_xml.append(
                        temp
                    )

                if not result["unmapped_db"].empty:
                    temp = (
                        result["unmapped_db"]
                        .copy()
                    )

                    temp.insert(
                        0,
                        "XML_FILE",
                        xml_file.name
                    )

                    all_unmapped_db.append(
                        temp
                    )

            except Exception as error:
                print(
                    f"ERROR - {xml_file.name}: "
                    f"{error}"
                )

                summary_rows.append(
                    create_error_summary(
                        xml_file,
                        error
                    )
                )

                if not CONTINUE_ON_ERROR:
                    raise

    finally:
        if conn is not None:
            conn.close()

            print(
                "\nSnowflake connection closed"
            )

    # ========================================================
    # CONSOLIDATED OUTPUT
    # ========================================================

    summary_df = pd.DataFrame(
        summary_rows
    )

    summary_df.to_csv(
        OUTPUT_FOLDER
        / "Validation_File_Summary.csv",
        index=False
    )

    if all_comparisons:
        pd.concat(
            all_comparisons,
            ignore_index=True
        ).to_csv(
            OUTPUT_FOLDER
            / "All_Comparisons.csv",
            index=False
        )

    if all_issues:
        pd.concat(
            all_issues,
            ignore_index=True
        ).to_csv(
            OUTPUT_FOLDER
            / "All_Mismatches.csv",
            index=False
        )

    if all_unmapped_xml:
        pd.concat(
            all_unmapped_xml,
            ignore_index=True
        ).to_csv(
            OUTPUT_FOLDER
            / "All_Unmapped_XML.csv",
            index=False
        )

    if all_unmapped_db:
        pd.concat(
            all_unmapped_db,
            ignore_index=True
        ).to_csv(
            OUTPUT_FOLDER
            / "All_Unmapped_Database.csv",
            index=False
        )

    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)

    if not summary_df.empty:
        print(
            summary_df.to_string(
                index=False
            )
        )

    print("\nOutput Folder:", OUTPUT_FOLDER)
    print("=" * 70)
    print("PROCESS COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    main()
