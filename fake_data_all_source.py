import pandas as pd
import numpy as np
from datetime import datetime
from random import randrange
from datetime import timedelta
import random
import string
import pyodbc

server = '20.212.35.255' 
database = 'Demo' 
username = 'ntncadmin' 
password = 'qaZwsXedC@2022' 
cnxn = pyodbc.connect('DRIVER={SQL SERVER};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)


def get_sales_order_item(length,key,sales_order_code):
    # With combination of lower and upper case
    result_str = ''.join(random.choice(string.ascii_letters) for i in range(length))
    # print random string
    return result_str + '_' + key + str(sales_order_code)

def random_date(start, end):
    """
    This function will return a random datetime between two datetime 
    objects.
    """
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def get_order_on_date(date):
    date = str(date)
    date = date.split(' ')[0]
    date = date.split('-')
    return ''.join(date)
def create_fake_data(df,company_wid_start,company_wid_end,year=2020):
    sales_order_code = 10000
    for company_wid in range(company_wid_start,company_wid_end+1):
    # company_wid = 3
        sql_product = 'select * from W_PRODUCT_D where company_wid = ' + str(company_wid)
        sql_customer = 'select * from W_CUSTOMER_D where company_wid = ' + str(company_wid)
        df_product= pd.read_sql(sql_product,cnxn)
        df_customer = pd.read_sql(sql_customer,cnxn)
        min_net = 2000000
        max_net = 9000000       ##1.1
        list_product_code = df_product['PRODUCT_CODE']
        list_product_name = df_product['PRODUCT_NAME']
        list_customer_code = df_customer['CUSTOMER_CODE']
        list_customer_name = df_customer['CUSTOMER_NAME']
        cursor = cnxn.cursor()
        cursor.execute("select  COMPANY_CODE from W_COMPANY_D where ROW_WID = " + str(company_wid))
        company_code = cursor.fetchone()[0]
        currency_code = 'VND'
        # order_qty = 1
        # year = 2020
        # # d1 = datetime.strptime('1/1/2008 1:30 PM', '%m/%d/%Y %I:%M %p')
        # # d2 = datetime.strptime('1/1/2009 4:50 AM', '%m/%d/%Y %I:%M %p')
        # # Random date
        date_t1 = []
        date_t2 = []
        date_t3 = []
        date_t4 = []
        date_t5 = []
        date_t6 = []
        date_t7 = []
        date_t8 = []
        date_t9 = []
        date_t10 = []
        date_t11 = []
        date_t12 = []
        for i in range(40):
            d1 = datetime.strptime(f'1/1/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d2 = datetime.strptime(f'1/2/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d3 = datetime.strptime(f'1/3/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d4 = datetime.strptime(f'1/4/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d5 = datetime.strptime(f'1/5/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d6 = datetime.strptime(f'1/6/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d7 = datetime.strptime(f'1/7/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d8 = datetime.strptime(f'1/8/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d9 = datetime.strptime(f'1/9/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d10 = datetime.strptime(f'1/10/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d11 = datetime.strptime(f'1/11/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d12 = datetime.strptime(f'1/12/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            d12_end = datetime.strptime(f'31/12/{str(year)} 1:30 PM', '%d/%m/%Y %I:%M %p')
            date_t1.append(random_date(d1,d2))
            date_t2.append(random_date(d2,d3))
            date_t3.append(random_date(d3,d4))
            date_t4.append(random_date(d4,d5))
            date_t5.append(random_date(d5,d6))
            date_t6.append(random_date(d6,d7))
            date_t7.append(random_date(d7,d8))
            date_t8.append(random_date(d8,d9))
            date_t9.append(random_date(d9,d10))
            date_t10.append(random_date(d10,d11))
            date_t11.append(random_date(d11,d12))
            date_t12.append(random_date(d12,d12_end))
        # date =date_t1+ date_t2 + date_t3 + date_t4 + date_t5 + date_t6 +date_t7 + date_t8 + date_t9 + date_t10 + date_t11+ date_t12
        date =date_t6 +date_t7 + date_t8 + date_t9 + date_t10 + date_t11+ date_t12
        for i in range(280):
            sales_order_item = get_sales_order_item(5,str(company_code[0:4]),sales_order_code)
            order_on_date = get_order_on_date(date[i])
            month_key = order_on_date[0:-2]
            product_random = random.randint(0, len(list_product_code)-1)
            product_code = list_product_code[product_random]
            product_name = list_product_name[product_random]
            if len(list_customer_code):
                customer_random = random.randint(0, len(list_customer_code)-1)
                customer_code = list_customer_code[customer_random]
                customer_name = list_customer_name[customer_random]
            else :
                customer_random = 0
                customer_code = ''
                customer_name = ''
            net_amount = random.randint(min_net,max_net)
            gross_amount = net_amount* 1.1
            tax_amount = net_amount*0.1
            create_on_date = date[i]
            change_on_date = create_on_date
            df_new = {'SALES_ORDER_CODE':[int(sales_order_code)],
            'SALES_ORDER_ITEM':[sales_order_item],
            'COMPANY_CODE':[company_code],
            'ORDER_ON_DATE':[order_on_date],
            'MONTH_KEY':[month_key],
            'PRODUCT_CODE':[product_code],
            'PRODUCT_NAME':[product_name],
            'CUSTOMER_CODE':[customer_code],
            'CUSTOMER_NAME':[customer_name],
            'ORDER_QTY':[1],
            'GROSS_AMOUNT':[gross_amount],
            'NET_AMOUNT':[net_amount],
            'DISCOUNT':[0],
            'TAX_AMOUNT':[tax_amount],
            'CURRENCY_CODE':[currency_code],
            'CREATE_ON_DATE':[create_on_date],
            'CHANGE_ON_DATE':[change_on_date]}
            df_new = pd.DataFrame(df_new)
            # df.append(df_new,ignore_index=True)
            df = pd.concat([df,df_new],ignore_index=True)
            sales_order_code += 1
    df.to_excel(f'Fake-Data-all-{str(year)}-source-{str(company_wid_start)}-2109.xlsx',index=False)

df = pd.DataFrame({
        'SALES_ORDER_CODE':[],
        'SALES_ORDER_ITEM':[],
        'COMPANY_CODE':[],
        'ORDER_ON_DATE':[],
        'MONTH_KEY':[],
        'PRODUCT_CODE':[],
        'PRODUCT_NAME':[],
        'CUSTOMER_CODE':[],
        'CUSTOMER_NAME':[],
        'STORE_CODE':[],
        'ORDER_QTY':[],
        'GROSS_AMOUNT':[],
        'NET_AMOUNT':[],
        'DISCOUNT':[],
        'TAX_AMOUNT':[],
        'BASE_UOM_CODE':[],
        'CURRENCY_CODE':[],
        'CREATE_ON_DATE':[],
        'CHANGE_ON_DATE':[]
    })
create_fake_data(df,1,1,year=2022)

