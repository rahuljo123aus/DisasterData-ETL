import psycopg2, sys

class postgres:
    def __init__(self, database='postgres', user='postgres', pasword='postgres', host='postgres', port='5432'):
        self.database="postgres"
        self.user="pguser"
        self.password="pgapwd"
        self.host="localhost"
        self.port="5445"
        self.cursor=None
    

    def getconnection(self):
        # Connect to the PostgreSQL database
        try:
            connection = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            # print("Connected to the database successfully")
            return connection
        except psycopg2.Error as e:
            print("Unable to connect to the database:", e)
            # Exit the script if connection fails
            sys.exit(-1)
    
    def execute_query(self, query=None):
        if query is None:
            print("[POSTGRES] [WARN]: Empty Query")
            return
        # Get Connection if required
        conn = self.getconnection()
        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Execute a command: this creates a new table
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print("[POSTGRES] [INFO]: ",query)
            print("[POSTGRES] [WARN]: ",e)
        finally:
            cur.close()
            conn.close()

    def execute_selectquery(self, query=None):
        if query is None:
            print("[POSTGRES] [WARN]: Empty Query")
            return
        # Get Connection if required
        conn = self.getconnection()
        # Open a cursor to perform database operations
        cur = conn.cursor()

        # Execute a command: this creates a new table
        try:
            cur.execute(query)
            records = cur.fetchall()
        except Exception as e:
            print("[POSTGRES] [WARN]: ",e)
        finally:
            cur.close()
            conn.close()
            return records
    
    def execute_batchquery(self, query=None):
        if query is None:
            print("[POSTGRES] [WARN]: Empty Query")
            return
        # Get Connection if required
        conn = self.getconnection()
        # Open a cursor to perform database operations
        cur = conn.cursor()
        # Execute a command: 
        try:
            for eachquy in query:
                cur.execute(eachquy)
        except Exception as e:
            print("[POSTGRES] [INFO]: ",eachquy)
            print("[POSTGRES] [WARN]: ",e)
        finally:
            cur.close()
            conn.close()

    