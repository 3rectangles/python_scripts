import csv
import sys
from datetime import datetime
import pymysql

host = None
dbUser  = None
dbPassword = None
dbName  = None
user = None
file_path = './grant.csv' 
cursorObject = None
connectionObject = None

def read_csv():
    tables = {}
    with open(file_path, mode='r') as f:
        reader = csv.reader(f)
        for row in reader:
            first_el = row[0]
            [table_name, col1] = first_el.split(":")
            tables[table_name] = []
            tables[table_name] += [col1] + row[1:]
    return tables

def make_mysql_connection():
    global cursorObject
    global connectionObject
    try:
        connectionObject = pymysql.connect(host=host, user=dbUser, password=dbPassword, db=dbName, port=3306)
        cursorObject = connectionObject.cursor()
    except:
        print(" connection to mysql serve failed")
    return cursorObject , connectionObject

def close_connection(cursorObject, connectionObject):
    cursorObject.close()
    connectionObject.close()

def generate_sql(cursorObject, csv_tables, dbname,  username):
    grants = []
    for t_name in csv_tables.keys():
        if not csv_tables[t_name]:
            sql = "GRANT SELECT ON " +dbname + "." + t_name + " TO "+ username + "@localhost;"
            grants.append(sql)
        else:
            str = ",".join(csv_tables[t_name])
            sql = "GRANT SELECT (" + str + ") ON " + dbName + "." + t_name+ " TO " + username + "@localhost;"
            grants.append(sql)
    return grants

def make_file(grants):
    date = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")
    filename = "grants_" + date + ".sql"
    f = open(filename, "x")
    with open(filename, 'w+') as f:
        for items in grants:
            f.write('%s\n' % items)
        print("File written successfully")
    f.close()

def main():
    csv_tables = read_csv()
    global host
    global dbUser
    global dbPassword
    global dbName
    global user
    try :
        host, dbUser , dbPassword, dbName, user =  sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
    except:
        print(" please provide all arguments in order: host, dbUser , dbPassword, dbName, user ")
        raise
    cursorObject  , connectionObject = make_mysql_connection()
    grants = generate_sql(cursorObject, csv_tables,  dbName, user)
    close_connection(cursorObject, connectionObject)
    make_file(grants)
    return 0

if __name__ == "__main__":
    main()
