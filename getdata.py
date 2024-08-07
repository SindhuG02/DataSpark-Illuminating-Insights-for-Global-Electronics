import pandas as pd
import mysql.connector
import warnings

warnings.simplefilter("ignore")
#connect to mySQL
con = mysql.connector.connect(
            host='localhost',
            user='root',
            password='12345678'
        )
cursor=con.cursor()

#create database if not present
query='create database if not exists Global_Electronics'
cursor.execute(query)
cursor.execute('use Global_Electronics')

#create tables if not present
query='''create table if not exists Exchange_Rates(
            Date varchar(20),
            Currency varchar(4),
            Exchange varchar(10))
    '''
cursor.execute(query)

query='''create table if not exists Customers(
            CustomerKey varchar(10),
            Gender varchar(10),
            Name varchar(50),
            City varchar(50),
            State_Code varchar(50),
            State varchar(50),
            Zip_Code varchar(10),
            Country varchar(50),
            Continent varchar(50),
            Birthday varchar(20),
            Age int)
    '''
cursor.execute(query)

query='''create table if not exists Products(
            ProductKey varchar(10),
            Product_Name varchar(100),
            Brand   varchar(20),
            Color varchar(20),
            Unit_Cost_USD varchar(10),
            Unit_Price_USD varchar(10),
            SubcategoryKey varchar(10),
            Subcategory varchar(50),
            CategoryKey varchar(10),
            Category varchar(50))
            
    '''
cursor.execute(query)

query='''create table if not exists Sales(
            Order_Number varchar(10),
            Line_Item varchar(10),
            Order_Date varchar(20),
            Delivery_Date varchar(20),
            CustomerKey varchar(20),
            ProductKey varchar(10),
            Quantity varchar(5),
            Currency_Code varchar(4))
            
    '''
cursor.execute(query)


#this this is the main method to clean the data
def cleandata(path,index):

    #just for safty we are creating the empty dataframe
    processed_data=pd.DataFrame()
    df=pd.DataFrame()
    processed_data=pd.DataFrame()
    #remove all the missing values
    
    #read csv file for pre processing
    df=pd.read_csv(path,encoding='unicode_escape')
    processed_data=df.dropna()
    
    if index==0:
        #for few date was not in correct format so converting the date column
        processed_data['Date']=pd.to_datetime(processed_data['Date'])
        
        #to store into db it was throwing error because db datetime is diffrent and dataframe date is different
        #and i picked easy way which is to convert to string and store.
        processed_data['Date'] = processed_data['Date'].astype(str)
        
        #to store into db we need data to be in tuple type
        data = [tuple(row) for row in processed_data.values]
        sql = "INSERT INTO Exchange_Rates (Date, Currency,Exchange) VALUES (%s, %s,%s)"

        cursor.executemany(sql, data)

    elif index==1:
        processed_data['Birthday']=pd.to_datetime(processed_data['Birthday'])
        processed_data['Birthday'] = processed_data['Birthday'].astype(str)
        Age=[]

        for i in processed_data['Birthday']:
            
            age=2024-(int(i[:4]))
            Age.append(age)
        processed_data['Age']=Age
        data = [tuple(row) for row in processed_data.values]
        sql = "INSERT INTO Customers (CustomerKey, Gender,Name,City,State_Code,State,Zip_Code,Country,Continent,Birthday,Age) VALUES (%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s)"
 
        cursor.executemany(sql, data)

    elif index==2:
        processed_data = processed_data.rename(columns={processed_data.columns[4]: 'Unit_Cost_USD'})
        processed_data = processed_data.rename(columns={processed_data.columns[5]: 'Unit_Price_USD'})
        
        usd=[]
        for i in processed_data['Unit_Price_USD']:
            usd.append(i[1:])
        processed_data['Unit_Price_USD']=usd
        percost=[]
        for i in processed_data['Unit_Cost_USD']:
            percost.append(i[1:])
        processed_data['Unit_Cost_USD']=percost
        data = [tuple(row) for row in processed_data.values]
        sql = "INSERT INTO Products (ProductKey, Product_Name,Brand,Color,Unit_Cost_USD,Unit_Price_USD,SubcategoryKey,Subcategory,CategoryKey,Category) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        
        cursor.executemany(sql, data)
        
    elif index==3:
        
        #column name should not have space inbetween name so replacing column names
        processed_data = processed_data.rename(columns={processed_data.columns[0]: 'Order_Number'})
        processed_data = processed_data.rename(columns={processed_data.columns[1]: 'Line_Item'})
        processed_data = processed_data.rename(columns={processed_data.columns[2]: 'Order_Date'})
        processed_data = processed_data.rename(columns={processed_data.columns[3]: 'Delivery_Date'})
        
        #converting to date type
        processed_data['Order_Date']=pd.to_datetime(processed_data['Order_Date'])
        processed_data['Delivery_Date']=pd.to_datetime(processed_data['Delivery_Date'])
        
        #converting to string back to store into db
        processed_data['Order_Date'] = processed_data['Order_Date'].astype(str)
        processed_data['Delivery_Date'] = processed_data['Delivery_Date'].astype(str)
        processed_data=processed_data.drop('StoreKey',axis=1)
        
        data = [tuple(row) for row in processed_data.values]
        sql = "INSERT INTO Sales (Order_Number, Line_Item,Order_Date,Delivery_Date,CustomerKey,ProductKey,Quantity,Currency_Code) VALUES (%s,%s,%s,%s,%s,%s,%s, %s)"
        
        cursor.executemany(sql, data)
 
    
    #save permentaly into db
    con.commit()

filepath=[r'C:\GUVI\Project 2_DataSpark\Exchange_Rates.csv',r'C:\GUVI\Project 2_DataSpark\Customers.csv',r'C:\GUVI\Project 2_DataSpark\Products.csv',r'C:\GUVI\Project 2_DataSpark\Sales.csv']
for i in range(len(filepath)):
    cleandata(filepath[i],i)