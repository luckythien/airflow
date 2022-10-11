from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
import functools
from airflow.utils.trigger_rule import TriggerRule
#docker build . --tag extending_airflow:latest

def change_datetime(data):
    import datetime
    return datetime.datetime.strptime(str(data), "%Y-%m-%dT%H:%M:%SZ")


def change_date(data):
    data = data.split("T")[0]
    data = data.split('/')
    data = "".join(data)
#     data = data.split('-')
#     data = "".join(data)
    return data


def change_birth(data):
    data = data.split("T")[0]
    data = data.split('/')
    data = "".join(data)
    data = data.split('-')
    data = "".join(data)
    return data


def random_phone(data):
    from random_phone import RandomUkPhone
    rukp = RandomUkPhone()
    if not data:
        data = rukp.random_premium()
    return data


def preload_history(source):
    import pymssql
    server = Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect(server=server, user=username,
                           password=password, database=database, port='1433')
    cur = conn.cursor()
    query = f"""EXEC PROC_INCREMENTAL_PRE_HISTORY '{source}'"""
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def preload_table(pkg_name, pkg_grp, table):
    import pymssql
    server = Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect(server=server, user=username,
                           password=password, database=database, port='1433')
    cur = conn.cursor()
    query = f"""EXEC PROC_INCREMENTAL_PRE_LOAD {pkg_name},{pkg_grp},{table}"""
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def postload_table(pkg_name):
    import pymssql
    server = Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect(server=server, user=username,
                           password=password, database=database, port='1433')
    cur = conn.cursor()
    query = f"""EXEC PROC_INCREMENTAL_POST_LOAD {pkg_name}"""
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


# def delete_staging_table(table_name):
#     import pymssql
#     server =  Variable.get('server')
#     database = Variable.get('database')
#     password = Variable.get('password')
#     username = Variable.get('username')
#     conn = pymssql.connect(server=server,user=username,password=password,database=database,port='1433')
#     cur = conn.cursor()
#     query = f"""DELETE FROM {table_name}"""
#     cur.execute(query)
#     conn.commit()
#     cur.close()
#     conn.close()

def execue_sql_query(sql_query):
    import pymssql
    server = Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect(server=server, user=username,
                        password=password, database=database, port='1433')
    cur = conn.cursor()
    # query = f"""DELETE FROM {table_name}"""
    cur.execute(sql_query)
    conn.commit()
    cur.close()
    conn.close()


def crawl_new_cus_data(ti):
    import pandas as pd
    from pdynamics import crm
    from random_phone import RandomUkPhone
    import random
    import pymssql
    import datetime
    try:
        crmurl = "https://antdev.crm5.dynamics.com/"
        user = "antd01@ntnc.vn"
        password = "DataTeam@123456"
        clientid = "1270c272-1ab3-4b86-9a34-8681e36dba68"
        clientsecret = "1o-o~.RDc81x1M.3R-W8TqoN7Kd2mA3_D."
        crmorg = crm.client(crmurl, user, password, clientid,
                            client_secret=clientsecret)
        crmorg.test_connection()
        QUERY_FULL = "accounts?$select=name,accountid,ant_dateofbirth,createdon,modifiedon,telephone1"
        data = crmorg.get_data(query=QUERY_FULL)
        data = data["value"]
        df = pd.DataFrame(data)
        df = df.fillna("")

        server = '20.212.35.255'
        database = 'Demo'
        password = 'qaZwsXedC@2022'
        username = 'ntncadmin'
        conn = pymssql.connect(server=server, user=username,
                            password=password, database=database, port='1433')
        cur = conn.cursor()
        query = """SELECT LAST_RUN_DT from W_PKG_TRACKING_F WHERE PKG_NAME = 'CustomerCRM' and STATUS = -1
                """
        cur.execute(query)
        last_date = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        last_date = str(last_date).split('.')[0]
        last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')
        df['createdon'] = df['createdon'].apply(change_datetime)
        df['modifiedon'] = df['modifiedon'].apply(change_datetime)
        df = df.drop(df[(df['createdon'] < last_date) &
                    (df['modifiedon'] < last_date)].index)

        birthday = df['ant_dateofbirth'].apply(change_birth)
        customer_code = df['accountid']
        company_code = ['ANT']*len(df)
        df['company_code'] = company_code
        integration_id = df['company_code'].astype(
            str) + "~" + "1" + "~" + df['accountid'].astype(str)
        name = df['name']
        datasoure_id = [1]*len(df)
        active_flg = [True]*len(df)
        delete_flg = [False]*len(df)
        createon = df['createdon@OData.Community.Display.V1.FormattedValue']
        modifiedon = df['modifiedon@OData.Community.Display.V1.FormattedValue']
        x_custom = [False]*len(df)
        df["telephone1"] = df["telephone1"].apply(random_phone)
        phone = df["telephone1"]
        df_cus = pd.DataFrame({
            'INTEGRATION_ID': integration_id,
            'DATASOURCE_ID': datasoure_id,
            'CUSTOMER_CODE': customer_code,
            'CUSTOMER_NAME': name,
            'COMPANY_CODE': company_code,
            'BIRTH_DAY': birthday,
            'PHONE': phone,
            'ACTIVE_FLG': active_flg,
            'DELETE_FLG': delete_flg,
            'CREATE_ON_DATE': createon,
            'CHANGE_ON_DATE': modifiedon,
            'X_CUSTOM': x_custom
        }
        )
        df_json_cus = df_cus.to_json()
        ti.xcom_push(key='df_cus', value=df_json_cus)
        print("this is dataframe", df_json_cus)
    except:
        ti.xcom_push(key='df_cus', value="False")

def crawl_new_product_data(ti):
    import pandas as pd
    from pdynamics import crm
    from random_phone import RandomUkPhone
    import random
    import pymssql
    import datetime
    crmurl = "https://antdev.crm5.dynamics.com/"
    user = "antd01@ntnc.vn"
    password = "DataTeam@123456"
    clientid = "1270c272-1ab3-4b86-9a34-8681e36dba68"
    clientsecret = "1o-o~.RDc81x1M.3R-W8TqoN7Kd2mA3_D."
    crmorg = crm.client(crmurl, user, password, clientid,
                        client_secret=clientsecret)
    crmorg.test_connection()
    QUERY_FULL = "ant_items?"
    data = crmorg.get_data(query=QUERY_FULL)
    data = data["value"]
    df = pd.DataFrame(data)
    df = df.fillna("")

    server = '20.212.35.255'
    database = 'Demo'
    password = 'qaZwsXedC@2022'
    username = 'ntncadmin'
    conn = pymssql.connect(server=server, user=username,
                           password=password, database=database, port='1433')
    cur = conn.cursor()
    query = """SELECT LAST_RUN_DT from W_PKG_TRACKING_F WHERE PKG_NAME = 'ProductCRM' and STATUS = -1
            """
    cur.execute(query)
    last_date = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    last_date = str(last_date).split('.')[0]
    last_date = datetime.datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')
    df['createdon'] = df['createdon'].apply(change_datetime)
    df['modifiedon'] = df['modifiedon'].apply(change_datetime)
    df = df.drop(df[(df['createdon'] < last_date) &
                 (df['modifiedon'] < last_date)].index)

    product_code = df['ant_itemid']
    company_code = ['ANT']*len(df)
    df['company_code'] = company_code
    integration_id = df['company_code'].astype(
        str) + "~" + "1" + "~" + df['ant_itemid']
    product_name = df['ant_name']
    product_status = df['statecode']
    datasoure_id = [1]*len(df)
    active_flg = [True]*len(df)
    delete_flg = [False]*len(df)
    createon = df['createdon@OData.Community.Display.V1.FormattedValue']
    modifiedon = df['modifiedon@OData.Community.Display.V1.FormattedValue']
    x_custom = [False]*len(df)
    df_pro = pd.DataFrame({
        'INTEGRATION_ID': integration_id,
        'DATASOURCE_ID': datasoure_id,
        'PRODUCT_CODE': product_code,
        'PRODUCT_NAME': product_name,
        'COMPANY_CODE': company_code,
        'PRODUCT_STATUS': product_status,
        'ACTIVE_FLG': active_flg,
        'DELETE_FLG': delete_flg,
        'CREATE_ON_DATE': createon,
        'CHANGE_ON_DATE': modifiedon,
        'X_CUSTOM': x_custom
    }
    )
    df_json_pro = df_pro.to_json()
    ti.xcom_push(key='df_product', value=df_json_pro)
    print("this is dataframe", df_json_pro)


def customercrm_to_staging(ti):
    import pandas as pd
    import random
    import pymssql
    df_json_cus = ti.xcom_pull(key='df_cus', task_ids='crawl_data_cus')
    df_cus = pd.read_json(df_json_cus)

    server = Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect(server=server, user=username,
                           password=password, database=database, port='1433')
    cur = conn.cursor()
    query = """
            INSERT INTO [dbo].[SW_CUSTOMER_D]
            ([INTEGRATION_ID]
            ,[DATASOURCE_ID]
            ,[CUSTOMER_CODE]
            ,[CUSTOMER_NAME]
            ,[COMPANY_CODE]
            ,[BIRTH_DAY]
            ,[PHONE]
            ,[ACTIVE_FLG]
            ,[DELETE_FLG]
            ,[CREATE_ON_DATE]
            ,[CHANGE_ON_DATE]
            ,[X_CUSTOM])
            VALUES (%s,%d,%s,%s,%s,%d,%d,%d,%d,%s,%s,%d)"""
    sql_data = tuple(map(tuple, df_cus.values))
    cur.executemany(query, sql_data)
    conn.commit()
    cur.close()
    conn.close()


def productcrm_to_staging(ti):
    import pandas as pd
    import random
    import pymssql
    df_json_pro = ti.xcom_pull(key='df_product', task_ids='crawl_data_pro')
    df_pro = pd.read_json(df_json_pro)

    server = Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect(server=server, user=username,
                            password=password, database=database, port='1433')
    cur = conn.cursor()
    query = """
            INSERT INTO [dbo].[SW_CUSTOMER_D]
            ([INTEGRATION_ID]
            ,[DATASOURCE_ID]
            ,[PRODUCT_CODE]
            ,[PRODUCT_NAME]
            ,[COMPANY_CODE]
            ,[PRODUCT_STATUS]
            ,[ACTIVE_FLG]
            ,[DELETE_FLG]
            ,[CREATE_ON_DATE]
            ,[CHANGE_ON_DATE]
            ,[X_CUSTOM])
            VALUES (%s,%d,%s,%s,%s,%d,%d,%d,%s,%s,%d)"""
    sql_data = tuple(map(tuple, df_pro.values))
    cur.executemany(query, sql_data)
    conn.commit()
    cur.close()
    conn.close()


# def customercrm_to_staging():
#     import pandas as pd
#     from pdynamics import crm
#     from random_phone import RandomUkPhone
#     import random
#     import pymssql

#     crmurl = "https://antdev.crm5.dynamics.com/"
#     user = "antd01@ntnc.vn"
#     password = "DataTeam@123456"
#     clientid = "1270c272-1ab3-4b86-9a34-8681e36dba68"
#     clientsecret = "1o-o~.RDc81x1M.3R-W8TqoN7Kd2mA3_D."
#     crmorg = crm.client(crmurl, user, password, clientid, client_secret=clientsecret)
#     crmorg.test_connection()
#     QUERY_FULL = "accounts?$select=name,accountid,ant_dateofbirth,createdon,modifiedon,telephone1"
#     data = crmorg.get_data(query=QUERY_FULL)
#     data = data["value"]
#     df = pd.DataFrame(data)
#     df = df.fillna("")
#     birthday = df['ant_dateofbirth'].apply(change_birth)
#     customer_code = df['accountid']
#     company_code = ['ANT']*len(df)
#     df['company_code'] = company_code
#     integration_id = df['company_code'] + "~" + "1" + "~" +df['accountid']
#     name = df['name']
#     datasoure_id = [1]*len(df)
#     active_flg = [True]*len(df)
#     delete_flg = [False]*len(df)
#     createon = df['createdon@OData.Community.Display.V1.FormattedValue']
#     modifiedon = df['modifiedon@OData.Community.Display.V1.FormattedValue']
#     x_custom = [False]*len(df)
#     df["telephone1"] = df["telephone1"].apply(random_phone)
#     phone = df["telephone1"]
#     df_cus = pd.DataFrame({
#         'INTEGRATION_ID':integration_id,
#         'DATASOURCE_ID' :datasoure_id,
#         'CUSTOMER_CODE' :customer_code,
#         'CUSTOMER_NAME':name,
#         'COMPANY_CODE':company_code,
#         'BIRTH_DAY':birthday,
#         'PHONE':phone,
#         'ACTIVE_FLG':active_flg,
#         'DELETE_FLG':delete_flg,
#         'CREATE_ON_DATE':createon,
#         'CHANGE_ON_DATE':modifiedon,
#         'X_CUSTOM':x_custom
#     }
#     )
#     server =  Variable.get('server')
#     database = Variable.get('database')
#     password = Variable.get('password')
#     username = Variable.get('username')
#     conn = pymssql.connect(server=server,user=username,password=password,database=database,port='1433')
#     cur = conn.cursor()
#     query="""
#             INSERT INTO [dbo].[SW_CUSTOMER_D]
#             ([INTEGRATION_ID]
#             ,[DATASOURCE_ID]
#             ,[CUSTOMER_CODE]
#             ,[CUSTOMER_NAME]
#             ,[COMPANY_CODE]
#             ,[BIRTH_DAY]
#             ,[PHONE]
#             ,[ACTIVE_FLG]
#             ,[DELETE_FLG]
#             ,[CREATE_ON_DATE]
#             ,[CHANGE_ON_DATE]
#             ,[X_CUSTOM])
#             VALUES (%s,%d,%s,%s,%s,%d,%d,%d,%d,%s,%s,%d)"""
#     sql_data = tuple(map(tuple,df_cus.values))
#     cur.executemany(query, sql_data)
#     conn.commit()
#     cur.close()
#     conn.close()

def view_tmp_cus():
    import pymssql
    import pandas as pd
    sql_query = """
        SELECT  [RECORD_STATUS],[INTEGRATION_ID],[DATASOURCE_ID]
        ,[CUSTOMER_CODE],[CUSTOMER_NAME],[COMPANY_WID],[BIRTH_DAY],[PHONE]
        ,[ACTIVE_FLG],[DELETE_FLG],[CREATE_ON_DATE],[CHANGE_ON_DATE],[X_CUSTOM],[LAST_UPDATED_DT]
        FROM ETL_W_CUSTOMER_D WHERE RECORD_STATUS IN ('I' , 'U')
    """

    server = Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect(server=server, user=username,
                            password=password, database=database, port='1433')
    cur = conn.cursor()
    df = pd.read_sql(sql_query, conn)
    query = """
            INSERT INTO [dbo].[TMP_W_CUSTOMER_D]
            ([RECORD_STATUS]
            ,[INTEGRATION_ID]
            ,[DATASOURCE_ID]
            ,[CUSTOMER_CODE]
            ,[CUSTOMER_NAME]
            ,[COMPANY_WID]
            ,[BIRTH_DAY]
            ,[PHONE]
            ,[ACTIVE_FLG]
            ,[DELETE_FLG]
            ,[CREATE_ON_DATE]
            ,[CHANGE_ON_DATE]
            ,[X_CUSTOM]
            ,[LAST_UPDATE_DATE])
            VALUES (%s,%s,%d,%s,%s,%d,%d,%s,%d,%d,%s,%s,%d,%s)"""
    sql_data = tuple(map(tuple, df.values))
    cur.executemany(query, sql_data)
    conn.commit()
    cur.close()
    conn.close()


def view_tmp_pro():
    import pymssql
    import pandas as pd
    sql_query = """
        SELECT [RECORD_STATUS],[INTEGRATION_ID],[DATASOURCE_ID],[PRODUCT_CODE],[PRODUCT_NAME]
        ,[COMPANY_WID],[PRODUCT_TYPE],[PRODUCT_BASIC],[PRODUCT_CAT_WID],[COLOR],[SIZE],[NET_WEIGHT],[VOLUMN]
        ,[UNIT_COST],[PRODUCT_STATUS],[ACTIVE_FLG],[DELETE_FLG]
        ,[CREATE_ON_DATE],[CHANGE_ON_DATE],[X_CUSTOM],[LAST_UPDATE_DATE]
        FROM [dbo].[ETL_W_PRODUCT_D]
        WHERE RECORD_STATUS IN ('U','I')
    """

    server = Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect( server=server, user=username,
                            password=password, database=database, port='1433')
    cur = conn.cursor()
    df = pd.read_sql(sql_query, conn)
    query = """
            INSERT INTO [dbo].[TMP_W_PRODUCT_D]
            ([RECORD_STATUS]
            ,[INTEGRATION_ID]
            ,[DATASOURCE_ID]
            ,[PRODUCT_CODE]
            ,[PRODUCT_NAME]
            ,[COMPANY_WID]
            ,[PRODUCT_TYPE]
            ,[PRODUCT_BASIC]
            ,[PRODUCT_CAT_WID]
            ,[COLOR]
            ,[SIZE]
            ,[NET_WEIGHT]
            ,[VOLUMN]
            ,[UNIT_COST]
            ,[PRODUCT_STATUS]
            ,[ACTIVE_FLG]
            ,[DELETE_FLG]
            ,[CREATE_ON_DATE]
            ,[CHANGE_ON_DATE]
            ,[X_CUSTOM]
            ,[LAST_UPDATE_DATE])
            VALUES (%s,%s,%d,%s,%s,%d,%s,%s,%d,%s,%s,%d,%d,%d,%s,%d,%d,%s,%s,%d,%s)"""
    sql_data = tuple(map(tuple, df.values))
    cur.executemany(query, sql_data)
    conn.commit()
    cur.close()
    conn.close()


def tmp_target_cus():
    import pymssql
    import pandas as pd
    sql_query = """
            SELECT [INTEGRATION_ID],[DATASOURCE_ID]
            ,[CUSTOMER_CODE],[CUSTOMER_NAME],[COMPANY_WID],[BIRTH_DAY]
            ,[PHONE],[ACTIVE_FLG],[DELETE_FLG]
            ,[CREATE_ON_DATE],[CHANGE_ON_DATE],[X_CUSTOM],GETDATE() as LAST_UPDATE_DATE
            FROM [dbo].[TMP_W_CUSTOMER_D] WITH (NOLOCK)
            WHERE RECORD_STATUS IN ('I')
            """

    server = Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect(server=server, user=username,
                        password=password, database=database, port='1433')
    df = pd.read_sql(sql_query, conn)
    cur = conn.cursor()
    query = """
            INSERT INTO [dbo].[W_CUSTOMER_D]
            ([INTEGRATION_ID]
            ,[DATASOURCE_ID]
            ,[CUSTOMER_CODE]
            ,[CUSTOMER_NAME]
            ,[COMPANY_WID]
            ,[BIRTH_DAY]
            ,[PHONE]
            ,[ACTIVE_FLG]
            ,[DELETE_FLG]
            ,[CREATE_ON_DATE]
            ,[CHANGE_ON_DATE]
            ,[X_CUSTOM]
            ,[LAST_UPDATE_DATE])
            VALUES (%s,%d,%s,%s,%d,%d,%s,%d,%d,%s,%s,%d,%s)"""
    sql_data = tuple(map(tuple, df.values))
    cur.executemany(query, sql_data)
    conn.commit()
    cur.close()
    conn.close()


def tmp_target_pro():
    import pymssql
    import pandas as pd
    sql_query = """
            SELECT  [RECORD_STATUS]
        ,[INTEGRATION_ID],[DATASOURCE_ID],[PRODUCT_CODE],[PRODUCT_NAME],[COMPANY_WID]
        ,[PRODUCT_TYPE],[PRODUCT_BASIC],[PRODUCT_CAT_WID],[COLOR]
        ,[SIZE],[NET_WEIGHT],[VOLUMN],[UNIT_COST]
        ,[PRODUCT_STATUS],[ACTIVE_FLG],[DELETE_FLG],[CREATE_ON_DATE]
        ,[CHANGE_ON_DATE],[X_CUSTOM],GETDATE() as LAST_UPDATE_DATE
        FROM [dbo].[TMP_W_PRODUCT_D] WITH (NOLOCK)
        WHERE RECORD_STATUS IN ('I')
            """

    server = Variable.get('server')
    database = Variable.get('database')
    password = Variable.get('password')
    username = Variable.get('username')
    conn = pymssql.connect(server=server, user=username,
                            password=password, database=database, port='1433')
    df = pd.read_sql(sql_query, conn)
    cur = conn.cursor()
    query = """
            INSERT INTO [dbo].[W_PRODUCT_D]
            ([INTEGRATION_ID]
            ,[DATASOURCE_ID]
            ,[PRODUCT_CODE]
            ,[PRODUCT_NAME]
            ,[COMPANY_WID]
            ,[PRODUCT_TYPE],[PRODUCT_BASIC],[PRODUCT_CAT_WID],[COLOR]
            ,[SIZE],[NET_WEIGHT],[VOLUMN],[UNIT_COST]
        ,[PRODUCT_STATUS],[ACTIVE_FLG],[DELETE_FLG],[CREATE_ON_DATE]
        ,[CHANGE_ON_DATE],[X_CUSTOM]
            ,[LAST_UPDATE_DATE])
            VALUES (%s,%d,%s,%s,%d,%s,%s,%d,%s,%s,%d,%d,%d,%s,%d,%d,%s,%s,%d)"""
    sql_data = tuple(map(tuple, df.values))
    cur.executemany(query, sql_data)
    conn.commit()
    cur.close()
    conn.close()

def _branch_cus(ti):
    condition = ti.xcom_pull(key='df_cus', task_ids='crawl_data_cus')
    if condition == 'False':
        return 'postload_table_customer_F'
    else:
        return 'load_cus_staging'

default_args = {
    'owner': 'coder2j',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id="dag_with_python_dependencies_v04",
    start_date=datetime(2021, 10, 12),
    schedule_interval='@once',
) as dag:

    preload_his = PythonOperator(
        task_id='preload_history_ANT',
        python_callable=functools.partial(preload_history, source='Ant'),
        dag=dag
    )

    # delete = PythonOperator(
    #     task_id = 'delete_staging',
    #     python_callable = functools.partial(delete_staging_table,table_name='SW_CUSTOMER_D'),
    #     dag=dag
    # )
    delete = PythonOperator(
        task_id='delete_staging',
        python_callable=functools.partial(execue_sql_query, sql_query="""DELETE FROM SW_CUSTOMER_D;
                                                                        DELETE FROM SW_PRODUCT_D;"""),
        dag=dag
    )

    preload_customer = PythonOperator(
        task_id='preload_table_customer',
        python_callable=functools.partial(
            preload_table, pkg_name='CustomerCRM', pkg_grp='Dim', table='W_CUSTOMER_D'),
        dag=dag
    )

    preload_product = PythonOperator(
        task_id='preload_table_product',
        python_callable=functools.partial(
            preload_table, pkg_name='ProductCRM', pkg_grp='Dim', table='W_PRODUCT_D'),
        dag=dag
    )

    crawl_data_cus = PythonOperator(
        task_id='crawl_data_cus',
        python_callable=crawl_new_cus_data,
        dag=dag
    )

    crawl_data_pro = PythonOperator(
        task_id='crawl_data_pro',
        python_callable=crawl_new_product_data,
        dag=dag
    )

    load_customer_staging = PythonOperator(
        task_id='load_cus_staging',
        python_callable=customercrm_to_staging,
        dag=dag
    )

    load_product_staging = PythonOperator(
        task_id='load_pro_staging',
        python_callable=productcrm_to_staging,
        dag=dag
    )

    delete_tmp_table_cus = PythonOperator(
        task_id='delete_tmp_customer',
        python_callable=functools.partial(
            execue_sql_query, sql_query='Delete from TMP_W_CUSTOMER_D'),
        dag=dag
    )

    delete_tmp_table_pro = PythonOperator(
        task_id='delete_tmp_pro',
        python_callable=functools.partial(
            execue_sql_query, sql_query='Delete from TMP_W_PRODUCT_D'),
        dag=dag
    )

    view_to_tmp_cus = PythonOperator(
        task_id='Transform_view_to_tmp_cus',
        python_callable=view_tmp_cus,
        dag=dag
    )

    view_to_tmp_pro = PythonOperator(
        task_id='Transform_view_to_tmp_pro',
        python_callable=view_tmp_cus,
        dag=dag
    )

    inserted_new_data_cus = PythonOperator(
        task_id='Inserted_new_data_to_target_table_cus',
        python_callable=tmp_target_cus,
        dag=dag
    )

    inserted_new_data_pro = PythonOperator(
        task_id='Inserted_new_data_to_target_table_pro',
        python_callable=tmp_target_pro,
        dag=dag
    )
    update_target_table_cus = PythonOperator(
        task_id='update_data_for_target_table_cus',
        python_callable=functools.partial(execue_sql_query, sql_query="""UPDATE D
                                                                        SET D.[CUSTOMER_CODE]		= DS.[CUSTOMER_CODE]
                                                                        ,D.[CUSTOMER_NAME]		= DS.[CUSTOMER_NAME]
                                                                        ,D.[COMPANY_WID]			= DS.[COMPANY_WID]
                                                                        ,D.[ACTIVE_FLG]			= DS.[ACTIVE_FLG]
                                                                        ,D.[DELETE_FLG]			= DS.[DELETE_FLG]
                                                                        ,D.[CREATE_ON_DATE]		= DS.[CREATE_ON_DATE]
                                                                        ,D.[CHANGE_ON_DATE]		= DS.[CHANGE_ON_DATE]
                                                                        ,D.[X_CUSTOM]				= DS.[X_CUSTOM]
                                                                        ,D.[LAST_UPDATE_DATE]		= DS.[LAST_UPDATE_DATE]
                                                                        FROM TMP_W_CUSTOMER_D DS
                                                                        INNER JOIN W_CUSTOMER_D D
                                                                        ON DS.INTEGRATION_ID = D.INTEGRATION_ID AND DS.DATASOURCE_ID = D.DATASOURCE_ID
                                                                        WHERE DS.RECORD_STATUS = 'U'"""),
        dag=dag
    )

    update_target_table_pro = PythonOperator(
        task_id='update_data_for_target_table_pro',
        python_callable=functools.partial(execue_sql_query, sql_query="""UPDATE D
                                                    SET		
                                                            D.[PRODUCT_CODE]       = DS.[PRODUCT_CODE]		 ,
                                                            D.[PRODUCT_NAME]	   = DS.[PRODUCT_NAME]		 ,
                                                            D.[COMPANY_WID]		   = DS.[COMPANY_WID]		 ,
                                                            D.[PRODUCT_TYPE]	   = DS.[PRODUCT_TYPE]		 ,
                                                            D.[PRODUCT_BASIC]	   = DS.[PRODUCT_BASIC]		 ,
                                                            D.[PRODUCT_CAT_WID]	   = DS.[PRODUCT_CAT_WID]	 ,
                                                            D.[COLOR]			   = DS.[COLOR]				 ,
                                                            D.[SIZE]			   = DS.[SIZE]				 ,
                                                            D.[NET_WEIGHT]		   = DS.[NET_WEIGHT]		 ,
                                                            D.[VOLUMN]			   = DS.[VOLUMN]			 ,
                                                            D.[UNIT_COST]		   = DS.[UNIT_COST]			 ,
                                                            D.[PRODUCT_STATUS]	   = DS.[PRODUCT_STATUS]	 ,
                                                            D.[ACTIVE_FLG]		   = DS.[ACTIVE_FLG]		 ,
                                                            D.[DELETE_FLG]		   = DS.[DELETE_FLG]		 ,
                                                            D.[CREATE_ON_DATE]	   = DS.[CREATE_ON_DATE]	 ,
                                                            D.[CHANGE_ON_DATE]	   = DS.[CHANGE_ON_DATE]	 ,
                                                            D.[X_CUSTOM]		   = DS.[X_CUSTOM]			 ,
                                                            D.[LAST_UPDATE_DATE]   = DS.[LAST_UPDATE_DATE]	 

                                                    FROM TMP_W_PRODUCT_D DS
                                                    INNER JOIN W_PRODUCT_D D
                                                    ON DS.INTEGRATION_ID = D.INTEGRATION_ID AND DS.DATASOURCE_ID = D.DATASOURCE_ID
                                                    WHERE DS.RECORD_STATUS = 'U'"""),
        dag=dag
    )

    postload_customer_success = PythonOperator(
        task_id='postload_table_customer_S',
        python_callable=functools.partial(
            postload_table, pkg_name='CustomerCRM'),
            
        dag=dag
    )
    postload_customer_failed = PythonOperator(
        task_id='postload_table_customer_F',
        python_callable=functools.partial(
            postload_table, pkg_name='CustomerCRM'),
            
        dag=dag
    )

    postload_product = PythonOperator(
        task_id='postload_table_product',
        python_callable=functools.partial(
            postload_table, pkg_name='ProductCRM'),
        dag=dag
    )

    postload_his = PythonOperator(
        task_id='postload_his',
        python_callable=functools.partial(
            execue_sql_query, sql_query='EXEC PROC_INCREMENTAL_POST_HISTORY Ant , 2'),
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag
    )

    send_mail_finish = EmailOperator(
        task_id='send_mail_finish',
        to='thien.hvt@ntnc.vn',
        subject='Job alert',
        html_content=""" <h3>Job ran successfully </h3> """
    )

    send_mail_cusF = EmailOperator(
        task_id='send_mail_cusF',
        to='thien.hvt@ntnc.vn',
        subject='Job alert',
        html_content=""" <h3>Crawl data from CRM for table Customer is failed </h3> """
    )

    send_mail_cusS = EmailOperator(
        task_id='send_mail_cusS',
        to='thien.hvt@ntnc.vn',
        subject='Job alert',
        html_content=""" <h3>Crawl data from CR< for table Customer is succeed</h3> """
    )

    branch = BranchPythonOperator(
            task_id='branch',
            python_callable = _branch_cus
        )

# preload_his >> delete >> preload_customer >> load_customer_staging >> delete_tmp_table >> view_to_tmp >> inserted_new_data >> update_target_table
preload_his >> delete >> [preload_customer, preload_product]

preload_customer >> crawl_data_cus 
crawl_data_cus >> branch >> [load_customer_staging,postload_customer_failed]

load_customer_staging >> delete_tmp_table_cus >> view_to_tmp_cus >> inserted_new_data_cus >> update_target_table_cus >> postload_customer_success >> send_mail_cusS
postload_customer_failed >> send_mail_cusF

preload_product >> crawl_data_pro >> load_product_staging >> delete_tmp_table_pro >> view_to_tmp_pro >> inserted_new_data_pro >> update_target_table_pro >> postload_product


[send_mail_cusS,send_mail_cusF, postload_product] >> postload_his >> send_mail_finish



#from airflow.operators.python import BranchPythonOperator
# ALL_SUCCESS = 'all_success'
# ALL_FAILED = 'all_failed'
# ALL_DONE = 'all_done'
# ONE_SUCCESS = 'one_success'
# ONE_FAILED = 'one_failed'
# DUMMY = 'dummy'


# load_customer_staging.set_upstream(preload_product)
# preload_his.set_downstream(delete)
