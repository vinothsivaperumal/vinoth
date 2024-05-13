
import logging
import sys
import getopt
import common.db as db
import common.custom_exceptions as cust_exp

def main(argv):
    
    chunk_size =5000
    # Set up logging
    logging.basicConfig(filename='{source_table}_error.log', level=logging.ERROR)
    
    try:
        # Define short and long options
        opts, args = getopt.getopt(sys.argv[1:], "s:", ["source_table="])
    except getopt.GetoptError:
        error_message = f"Error: {getopt.GetoptError}"
        logging.error(f"Exception Type: {type(getopt.GetoptError).__name__}, Message: {error_message}")
        sys.exit(2)
    
    for opt, arg in opts:
        if opt in ("-s", "--source_table"):
            source_table = arg

    try:
        # Load database configurations
        
        # Connect to source and destination Oracle databases
        source_conn = db.db_connect("local","source")
        dest_conn = db.db_connect("local","destination")  
        # Check if the table exists
        if not db.check_table_exists(source_conn,source_table):
            raise cust_exp.TableNotFoundError(f"Source table '{source_table}' not found in the source system.")
        
        if not db.check_table_exists(dest_conn,source_table):
            db.create_table_source(source_conn,dest_conn,source_table)

        # Build SQL query
        source_query = db.build_query(source_table) 

        # Copy data from source to destination
        db.copy_data(source_conn, dest_conn, source_query, source_table,chunk_size)

        # Close connections
        source_conn.close()
        dest_conn.close()

    except Exception as e:
        error_message = f"Error: {e}"
        logging.error(f"Exception Type: {type(e).__name__}, Message: {error_message}")
    except cust_exp.TableNotFoundError as tnfe:
        error_message = f"Error: {tnfe}"
        logging.error(f"Exception Type: {type(tnfe).__name__}, Message: {error_message}")

if __name__ == "__main__":
    main()