from sqlalchemy import create_engine,text
from loguru import logger
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def connection(username,password):
    try:
        #creating engine
        engine=create_engine(f'mysql+pymysql://{username}:{password}@localhost/ecommers')

        logger.info("engine created")
        return engine
    except Exception as e:
        logger.error(f"Error: {e}")
        return None
 #insert data into table
def cleandata(df):
   #drop dublicated
   df.drop_duplicates(inplace=True)
   #drop na columns
   df.dropna(axis=1,inplace=True,thresh=10)
   # fill emmpty
   df = df.ffill().bfill()
   return df
def visuals(query):
    with engine.connect() as connection:
        result=connection.execute(query) 
    return result 
       
if __name__=="__main__":
    username=os.environ.get('SQLUSER')
    password=os.environ.get("SQLPASS")
    #calling connection function
    engine=connection(username,password)
    
    customer_df=pd.read_csv("C://Users//arsha//Desktop//HW//___projects____//resource//customer_dimension.csv")
    date_df=pd.read_csv("C://Users//arsha//Desktop//HW//___projects____//resource//date_dimension.csv")
    inventory_df=pd.read_csv("C://Users//arsha//Desktop//HW//___projects____//resource//inventory_fact.csv")
    location_df=pd.read_csv("C://Users//arsha//Desktop//HW//___projects____//resource//location_dimension.csv")
    order_df=pd.read_csv("C://Users//arsha//Desktop//HW//___projects____//resource//orders_fact.csv")
    product_df=pd.read_csv("C://Users//arsha//Desktop//HW//___projects____//resource//product_dimension.csv")
    salse_df=pd.read_csv("C://Users//arsha//Desktop//HW//___projects____//resource//sales_fact.csv")
    #original dataframe list
    df_list=[customer_df,date_df,inventory_df,location_df,order_df,product_df,salse_df]

    #newlist containe cleand dataframes-*-
    newdatalist=[]
    
    #lopping over dataframe to cleandata function
    for df in df_list:
       cdf=cleandata(df)
       newdatalist.append(cdf)
     
     #Table names in database  
    namelist=['customer_df','date_df','inventory_df','location_df','order_df','product_df','salse_df']
    #writing dataframe to databse as per namelist names
    for x in range(len(namelist)):
       newdatalist[x].to_csv(f"C://Users//arsha//Desktop//HW//___projects____//outputresource//{namelist[x]}.csv")
       newdatalist[x].to_sql(namelist[x],con=engine,if_exists="replace",index=False)
    print("done")
  
    
    #highest sales in year
    query=text("select max(sales_df.SalesAmount) as maxsale,date_df.Year  from sales_df  inner join date_df on sales_df.DateID=date_df.DateID group by  date_df.Year order by date_df.Year")
    maxproduct=text("select max(sales_df.Profit)as profit , product_df.Name from sales_df inner join product_df on sales_df.ProductID=product_df.ProductID group by product_df.Name")
    result=visuals(maxproduct)
    pdf=pd.DataFrame(result.fetchall())
    #higheset year
    # plt.bar(pdf['Year'],pdf['maxsale'])
    y=pdf['profit']
    x=pdf['Name']
    
    plt.title("max sale per year")
    plt.ylabel("max sale")
    plt.xlabel("year")
    plt.plot(x,y)
    plt.grid()
    plt.show()
 
        
        
      
   
   
  
     
    
    

    
   


   
    
   
   
   
   
        
    
        
        
        
        
    
    
    
    