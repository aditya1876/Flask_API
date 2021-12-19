from flask import Flask, render_template, request, jsonify

import pymongo
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


'''
Objective:
    - Create a UI to connect to cloud DBs (MongoDb and Cassandra)
    - Create APIs to do CRUD operations to these DBs
'''

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST']) # To render Homepage
def home_page():
    return render_template('index.html')

@app.route('/do_operation', methods=['POST'])
def do_operation():
    if (request.method=='POST'):
        operation=request.form['operation']
        db_type=request.form['db_type']
        username=request.form['username']
        password=request.form['password']
        table_name=request.form['table_name']
        column_names=request.form['column_names']
        column_datatypes=request.form['column_datatypes']
        column_values=request.form['column_values']
        file_location=request.form['file_location']
        download_table=request.form['download_table']
        l1=column_names.split(',')
        l2=column_datatypes.split(',')
        l3=column_values.split(',')

        if (operation=='CreateTable'):            
            
            l4=""
            for i in range(len(l1)):
                l4=l4+" "+str(l1[i])+" "+l2[i]+","

            #create DB table
            try:
                if (db_type=='MongoDB'):
                    #create connection
                    conn="mongodb+srv://"+username+":"+password+"@cluster0.93mth.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
                    client = pymongo.MongoClient(conn)
                    #create table/collection inside 'myDB'
                    db_cloud=client['myDB']
                    coll1=db_cloud[table_name]
                    result = "Table "+table_name+" in "+db_type+" created cuccessfully!"

                    return render_template('results.html',result=result)
                elif(db_type=='Cassandra'):
                    #cassandra code
                    cloud_config= {'secure_connect_bundle': '/home/adi01/01_Code/Ineuron_Course/Databases/secure-connect-testdb.zip'} 
                    auth_provider = PlainTextAuthProvider(username, password)
                    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                    session = cluster.connect()

                    #create Keyspace
                    row=session.execute("CREATE KEYSPACE home WITH replication={'class':'SimpleStrategy','replication_factor':4};")
                    #use keyspace
                    row=session.execute("use home;")
                    #creating table in the keyspace
                    query="CREATE TABLE "+table_name+" ("+l3+");"
                    row=session.execute(query)

                    result = "Table "+table_name+" in "+db_type+" created successfully!"
                    return render_template('results.html',result=result)
            except Exception as e:
                print(str(e))
        elif(operation=='Update'):
            record={}
            if (db_type=='MongoDB'):

                for i in range(len(l1)):
                    record[l1[i]]=l3[i]
                
                #create connection
                conn="mongodb+srv://"+username+":"+password+"@cluster0.93mth.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
                client = pymongo.MongoClient(conn)
                #create table/collection inside 'myDB'
                db_cloud=client['myDB']
                coll1=db_cloud[table_name]

                #coll1.updateOne({
                #    {
                #        $set: record
                #    }
                #})              

                result = "Table "+table_name+" in "+db_type+" updated successfully!"
                return render_template('results.html',result=result)
            elif(db_type=='Cassandra'):
                #cassandra code
                cloud_config= {'secure_connect_bundle': '<file_location>'} 
                auth_provider = PlainTextAuthProvider(username, password)
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                session = cluster.connect()

                #create Keyspace
                row=session.execute("CREATE KEYSPACE home WITH replication={'class':'SimpleStrategy','replication_factor':4};")
                #use keyspace
                row=session.execute("use home;")
                #creating table in the keyspace
                query="CREATE TABLE "+table_name+" ("+l3+");"
                row=session.execute(query)

                result = "Table "+table_name+" in "+db_type+" created successfully!"
                return render_template('results.html',result=result)
        elif(operation=='Insert'):
            if (db_type=='MongoDB'):
                for i in range(len(l1)):
                    record[l1[i]]=l3[i]
                
                #create connection
                conn="mongodb+srv://"+username+":"+password+"@cluster0.93mth.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
                client = pymongo.MongoClient(conn)
                #create table/collection inside 'myDB'
                coll1=myDB[table_name]
                coll1.insert_one(record)
                
                result = "Table "+table_name+" in "+db_type+" inserted successfully!"
                return render_template('results.html',result=result)
            elif(db_type=='Cassandra'):

                result = "Table "+table_name+" in "+db_type+" inserted successfully!"
                return render_template('results.html',result=result)
        elif(operation=='Delete'):
            if (db_type=='MongoDB'):
                for i in range(len(l1)):
                    record[l1[i]]=l3[i]
                
                #create connection
                conn="mongodb+srv://"+username+":"+password+"@cluster0.93mth.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
                client = pymongo.MongoClient(conn)
                #create table/collection inside 'myDB'
                coll1=myDB[table_name]

                coll1.delete_one(record)

                result = "Table "+table_name+" in "+db_type+" deleted successfully!"
                return render_template('results.html',result=result)
            elif(db_type=='Cassandra'):

                result = "Table "+table_name+" in "+db_type+" deleted successfully!"
                return render_template('results.html',result=result)
        elif(operation=='BulkInsert'):
            if (db_type=='MongoDB'):
                
                result = "Table "+table_name+" in "+db_type+" bulkInserted successfully!"
                return render_template('results.html',result=result)
            elif(db_type=='Cassandra'):

                result = "Table "+table_name+" in "+db_type+" bulkinserted successfully!"
                return render_template('results.html',result=result)
        elif(operation=='Download'):
            if (db_type=='MongoDB'):
                
                result = "Table "+table_name+" in "+db_type+" downloaded successfully!"
                return render_template('results.html',result=result)
            elif(db_type=='Cassandra'):

                result = "Table "+table_name+" in "+db_type+" downloaded successfully!"
                return render_template('results.html',result=result)


if __name__ == '__main__':
    app.run()
