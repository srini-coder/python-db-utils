import yaml
import psycopg2

def execute_queries(config_file):
    # Read YAML configuration file
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    # Connect to Redshift
    conn = psycopg2.connect(
        dbname=config['redshift']['dbname'],
        user=config['redshift']['user'],
        password=config['redshift']['password'],
        host=config['redshift']['host'],
        port=config['redshift']['port']
    )
    cursor = conn.cursor()

    # Execute queries
    for query_name in config['queries']:
        query = config['queries'][query_name]
        print(f"Executing query: {query_name}")
        try:
            cursor.execute(query)
            conn.commit()
            print("Query executed successfully.")
        except Exception as e:
            print(f"Error executing query {query_name}: {str(e)}")

    # Close connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    config_file = "queries.yaml"
    execute_queries(config_file)
