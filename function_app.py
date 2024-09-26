

import azure.functions as func
import logging
import os, json
import psycopg2
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
accessToken = credential.get_token('https://ossrdbms-aad.database.windows.net/.default').token

DB_HOST = os.getenv('DB_HOST', 'pg-srv-001.postgres.database.azure.com') 
DB_NAME = os.getenv('DB_NAME', 'db1')  
DB_USER = os.getenv('DB_USER', 'test-fa-abc-01') 

app = func.FunctionApp()

@app.route(route="http_trigger",  methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS )
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )


def write_to_db(name, age):

    try:
        conn_string = f"postgresql://{DB_USER}:{accessToken}@{DB_HOST}/{DB_NAME}?sslmode=require"
        conn = psycopg2.connect(conn_string) 
        cur = conn.cursor()

        cur.execute("INSERT INTO persons (name, age) VALUES (%s, %s)", (name, age))
        conn.commit()

        cur.close()
        conn.close()

        return "Data inserted successfully"
    except Exception as e:
        return str(e)
    

@app.route(route="write_data", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS )
def write_data(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
        name = req_body.get('name')
        age = req_body.get('age')

        if not name or not age:
            return func.HttpResponse("Invalid input. Make sure to send name and age.", status_code=400)

        result = write_to_db(name, age)
        return func.HttpResponse(result, status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    

### READ DATA

def read_from_db():

    try:
        # Connect to the PostgreSQL database
        conn_string = f"postgresql://{DB_USER}:{accessToken}@{DB_HOST}/{DB_NAME}?sslmode=require"
        conn = psycopg2.connect(conn_string) 
        cur = conn.cursor()

        cur.execute("SELECT id, name, age FROM persons;")
        rows = cur.fetchall()

        cur.close()
        conn.close()

        # Convert data to a list of dictionaries for JSON serialization
        results = []
        for row in rows:
            results.append({
                'id': row[0],
                'name': row[1],
                'age': row[2]
            })

        return json.dumps(results)

    except Exception as e:
        return str(e)


@app.route(route="read_data", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS )
def read_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Fetch data from the database
        data = read_from_db()
        return func.HttpResponse(data, mimetype="application/json", status_code=200)
        logging.info('Date read completed.')

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)


## Initialize Database
def initialize_database():

    try:
        conn_string = f"postgresql://{DB_USER}:{accessToken}@{DB_HOST}/{DB_NAME}?sslmode=require"
        conn = psycopg2.connect(conn_string) 
        # psycopg2.connect(
        #     host=DB_HOST,
        #     database=DB_NAME,
        #     user=DB_USER,
        #     password=accessToken,
        #     sslmode="require"
        # )
        cur = conn.cursor()
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS persons (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            age INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        '''

        cur.execute(create_table_query)
        conn.commit()

        cur.close()
        conn.close()

        return "Table 'persons' created or already exists."

    except Exception as e:
        logging.error(f"Error initializing database: {str(e)}")
        return str(e)
    
@app.route(route="initialize_db", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS )
def initialize_db(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        result = initialize_database()
        return func.HttpResponse(result, status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)