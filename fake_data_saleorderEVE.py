import pandas as pd
import numpy as np
from datetime import datetime
from random import randrange
from datetime import timedelta
import random
import string

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

dir_source = 'Data-eve-flower-update.xlsx'
df_product= pd.read_excel(dir_source,sheet_name='PRODUCT')
df_customer = pd.read_excel(dir_source,sheet_name='CUSTOMER')

min_net = 2000000
max_net = 9000000       ##1.1
list_product_code = df_product['PRODUCT_CODE']
list_product_name = df_product['PRODUCT_NAME']
list_customer_code = df_customer['CUSTOMER_CODE']
list_customer_name = df_customer['CUSTOMER_NAME']
company_code = 'EVEFLOWER'
currency_code = 'VND'
order_qty = 1
sales_order_code = 5020
# d1 = datetime.strptime('1/1/2008 1:30 PM', '%m/%d/%Y %I:%M %p')
# d2 = datetime.strptime('1/1/2009 4:50 AM', '%m/%d/%Y %I:%M %p')
# Random date
date_t5 = []
date_t6 = []
date_t7 = []
date_t8 = []
date_t9 = []
for i in range(25):
    d1 = datetime.strptime('1/5/2022 1:30 PM', '%d/%m/%Y %I:%M %p')
    d2 = datetime.strptime('1/6/2022 1:30 PM', '%d/%m/%Y %I:%M %p')
    d3 = datetime.strptime('1/7/2022 1:30 PM', '%d/%m/%Y %I:%M %p')
    d4 = datetime.strptime('1/8/2022 1:30 PM', '%d/%m/%Y %I:%M %p')
    d5 = datetime.strptime('1/9/2022 1:30 PM', '%d/%m/%Y %I:%M %p')
    d6 = datetime.strptime('1/10/2022 1:30 PM', '%d/%m/%Y %I:%M %p')
    date_t5.append(random_date(d1,d2))
    date_t6.append(random_date(d2,d3))
    date_t7.append(random_date(d3,d4))
    date_t8.append(random_date(d4,d5))
    date_t9.append(random_date(d5,d6))

date = date_t5 + date_t6 +date_t7 +date_t8 + date_t9

for i in range(125):
    sales_order_item = get_sales_order_item(5,'EVE',sales_order_code)
    order_on_date = get_order_on_date(date[i])
    month_key = order_on_date[0:-2]
    product_random = random.randint(0, len(list_product_code)-1)
    product_code = list_product_code[product_random]
    product_name = list_product_name[product_random]
    customer_random = random.randint(0, len(list_customer_code)-1)
    customer_code = list_customer_code[customer_random]
    customer_name = list_customer_name[customer_random]
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
df.to_excel('Data-Eve-09-2022.xlsx',index=False)


