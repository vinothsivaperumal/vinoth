def parse_file_format_options(input_str):
    """
    Parse Snowflake FILE_FORMAT options from a given input string into a dictionary.
    
    Args:
        input_str (str): Input string containing file format options.
        
    Returns:
        dict: Parsed file format options as a dictionary.
    """
    # Initialize an empty dictionary to store file format options
    file_format_options = {}
    
    # Split the input string by spaces to get individual key-value pairs
    items = input_str.split()
    
    i = 0
    while i < len(items):
        key = items[i].strip()  # Get the key and remove any leading/trailing whitespace
        
        # Check if the key is followed by '=' and a value
        if i + 2 >= len(items) or items[i + 1].strip() != '=':
            i += 1
            continue
        
        value = items[i + 2].strip("'")  # Get the value and remove any leading/trailing single quotes
        
        # Convert specific values to Python types
        if value == 'FALSE':
            value = False
        elif value == 'TRUE':
            value = True
        elif value == '()':
            value = ()
        elif value.startswith("'") and value.endswith("'"):
            value = value[1:-1]  # Remove leading/trailing single quotes
        
        # Remove trailing '=' from the key and add key-value pair to dictionary
        key = key.rstrip('=')
        file_format_options[key] = value
        
        # Move to the next key-value pair
        i += 3
    
    return file_format_options

def map_to_csv_to_copy_into(options):
    copy_into_params = {}

    # Mapping Snowflake FILE_FORMAT options to pandas to_csv parameters
    mapping = {
        'FIELD_DELIMITER': 'sep',
        'NULL_IF': 'na_rep',
        'FIELD_OPTIONALLY_ENCLOSED_BY': 'quotechar',
        'RECORD_DELIMITER': 'line_terminator',
        'ESCAPE': 'escapechar',
        'UTF8': 'encoding',
        'COMPRESSION': 'compression',
        'DECIMAL': 'decimal',
        'DATE_FORMAT': 'date_format',
        'TIME_FORMAT': 'time_format',
        'INCLUDE_COLUMN_NAMES': 'columns',
    }

    # Map options to corresponding pandas to_csv parameters
    for key, value in options.items():
        if key in mapping:
            copy_into_params[mapping[key]] = value

    # Set default values for parameters not specified in FILE_FORMAT options
    copy_into_params.setdefault('header', True)  # Default header to True
    copy_into_params.setdefault('index', False)  # Default index to False

    return copy_into_params

# Example usage
input_str = "TYPE = CSV COMPRESSION = NONE FIELD_DELIMITER = '|' EMPTY_FIELD_AS_NULL = FALSE NULL_IF = ()"
file_format_options = parse_file_format_options(input_str)
copy_into_params = map_to_csv_to_copy_into(file_format_options)

print(copy_into_params)
