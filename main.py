import requests
import json
import pandas as pd
import jaydebeapi

def call(url):
    try:
        print("API callin...")
        response=requests.get(url)
        print("API call successful...")
        return response.json()
    except Exception as e:
        print(f"Error : {e}")
  

def writing_to_mysql(username,password):
    try:
        print("trying to connect with mysql...")
        conn = jaydebeapi.connect(
            "com.mysql.cj.jdbc.Driver",                                              # 1. JDBC Driver Class
            f"jdbc:mysql://localhost:3306/mydb?user=root&password=8907",             # 2. JDBC Connection URL
            [username,password],                                                     # 3. Username and Password
            "C:/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar",                # 4. Path to JDBC Driver JAR File
        )
        print("connection sucessfull..")
        return conn
    except Exception as e:
        print(f'Error :{e}')
    

if __name__ == '__main__':
    
    url="https://jsonplaceholder.typicode.com/comments"
    username = "root"
    password = "8907"

    data=call(url)
    
    df = pd.DataFrame(data)
    
    conn=writing_to_mysql(username,password,)
    cursor=conn.cursor()
    
    create_table="create table if not exists comments (postid int ,id int ,name varchar(255),email varchar(255),body varchar(1000))"
    insert_data="insert into comments (postid,id,name,email,body) values(?,?,?,?,?)"
    cursor.execute(create_table)
    try:
        for _, row in df.iterrows():
        # Insert data into the database
            cursor.execute(insert_data,(int(row['postId']), int(row['id']), row['name'], row['email'], row['body']))
        print("Data inserted successfully")  
    except Exception as e:
        print(f"Error: {e}")
  
    

   
    
  
     
     
   
    

    
    