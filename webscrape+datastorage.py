import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from loguru import logger

@logger.catch
def mysqlcon(username,password):
    # Create a connection to the database
    try:
        engine = create_engine(f'mysql+pymysql://{username}:{password}@localhost/mydb')
        logger.info("connection sucessfull")
        return engine
    except Exception as e:
        logger.exception(f"connection Fails:{e}")
        return None
def getresponse(url,params):
    try:
        logger.info("getting response")
        response=requests.get(url,params=params)
        if response.status_code==200:
            logger.info("parsering response")
            response.encoding = 'utf-8'
            
            soup=BeautifulSoup(response.content,"html.parser")
            product_detail=soup.find_all("a",class_="a-link-normal s-line-clamp-4 s-link-style a-text-normal")
            logger.info("done parsing")
        return product_detail
    except exception as e:
        logger.exception(f"getting response fails:{e}")
        return None

def getdata(product_detail,data):
    try:
        logger.info("start scraping..")
        for link in product_detail:
            href=link.get('href')
            if href:
                product_link=f'https://www.amazon.com{href}'
                product_response=requests.get(product_link,params=params)
                if product_response.status_code==200:
                    product_soup=BeautifulSoup(product_response.content,'html.parser')
                    # product title
                    title=product_soup.find('span',class_="a-size-large product-title-word-break").text.strip()
                    #product price
                    price=product_soup.find("span",class_="a-offscreen").text.strip()
                    #product description
                    description=product_soup.find("li",class_="a-spacing-mini")
                    if description:
                         p_description = description.text.strip()
                    else:
                        print("")    
                    #product reviews
                    reviews=product_soup.find("div",class_="a-expander-content reviewText review-text-content a-expander-partial-collapse-content").find("span").text.strip()
                    #product rating
                    rating=product_soup.find("span",class_="a-size-base a-nowrap").find("span").text.strip()
                    #product image
                    image=product_soup.find("div",class_="imgTagWrapper").find("img")
                    imgsrc=image.get("src")
                    
                    # print(p_description)
                    
                    data["product_name"].append(title) 
                    data["price"].append(price)
                    data["description"].append(p_description)
                    data["reviews"].append(reviews)
                    data["rating"].append(rating)
                    data["image"].append(imgsrc)
        logger.info("parsing")            
        return data
    except exception as e:
        logger.exception(f"fail to get data:{e}")
        return None

if __name__== "__main__":

    url='https://proxy.scrapeops.io/'
    params={
        'api_key': '6c284ef7-4a23-4c-a6e4-aeb0562dc2e1',
        'url': 'https://www.amazon.com/s?k=gaming'
    }
    
    username="root"
    password="8907"
    data={"product_name":[],"price":[],"description":[],"reviews":[],"rating":[],"image":[]}

    product_detail=getresponse(url,params)
    
    scrap_data=getdata(product_detail,data)
    
    amazon_df=pd.DataFrame.from_dict(scrap_data)
   
    engine=mysqlcon(username,password)
    try:
        amazon_df.to_sql('amazon_product',engine,if_exists='replace',index=False)
        logger.info("data trafered sucessfully")
    except Exception as e:
        logger.exception(f"failed:{e}")
            
    
   

                
                
        
              
               
               
               
           
