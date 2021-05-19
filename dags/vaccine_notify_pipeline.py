from airflow import DAG
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta
import pandas as pd
import requests

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def get_resp(dist_cd, url, headers):
    """
    This function iterates through prefered dates for given districts against cowin website
    :param dist_cd: This is the district code
    :param url: cowin get api url
    :param headers: request headers
    :return: returns response of vaccine centeres found in the given district
    """
    response = {'sessions': []}
    #Modify the time range as per your preference. Here we are searching from 17th to 30th May, 2021 time slots
    for i in range(17, 30):
        params = (
            ('district_id', dist_cd),
            ('date', str(i) + '-05-2021'),
        )
        response1 = requests.get(url,
                                 headers=headers, params=params)
        # print(response1.json())
        sessions = response1.json()['sessions']
        if (len(sessions) > 0):
            response['sessions'].extend(sessions)
    return response



def get_slots(**kwargs):
    #This BASE_URL can be added to variables list in airflow
    BASE_URL = 'https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/findByDistrict'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36',
    }
    response1 = get_resp('603', BASE_URL, headers)
    response2 = get_resp('581', BASE_URL, headers)
    new_resp = {'sessions': []}
    new_resp['sessions'].extend(response1['sessions'])
    new_resp['sessions'].extend(response2['sessions'])
    final_resp = {}
    required_keys = {'name', 'available_capacity_dose1', 'available_capacity_dose2', 'district_name', 'pincode', 'date', 'min_age_limit', 'vaccine'}
    for i_dict in new_resp['sessions']:
        #Modify this condition as per your preference. If you want to get only updates related to age then put min_age_limit condition.
        if (i_dict['available_capacity_dose1'] > 0 or i_dict['available_capacity_dose2']>0):
            final_resp[i_dict['name']] = {required_key: i_dict[required_key] for required_key in required_keys}

    kwargs['ti'].xcom_push(key='value from pusher 1', value=final_resp)

def _choose_next_task(ti):
    get_slot_resp = ti.xcom_pull(key='value from pusher 1', task_ids=['get_slots'][0])
    print(type(get_slot_resp))
    print(len(get_slot_resp))
    print(get_slot_resp)
    if len(get_slot_resp) > 0:
        return 'email_task'
    return 'skipped_task'

def notify_email(ti):
    """Send custom email alerts."""
    email_dl = Variable.get("email_dl")
    response = ti.xcom_pull(key='value from pusher 1', task_ids=['get_slots'][0])
    # email title.
    title = "Slots Available notification "
    a = response
    df = pd.DataFrame(data=a)
    df = df.fillna(' ').T
    df = df.applymap(lambda x: x[0] if type(x) == list else x)
    body = df.to_html()
    print('the email_dl is', email_dl)
    send_email(email_dl, title, body)

#The Dag runs for every 5 mins, customize as per your preference
with DAG("vaccine_notify_pipeline", start_date=datetime(2021, 1, 1),
         schedule_interval="*/5 * * * *", default_args=default_args, catchup=False) as dag:


    get_slots = PythonOperator(
        task_id="get_slots",
        provide_context=True,
        python_callable=get_slots
    )

    choose_next_task = BranchPythonOperator(
        task_id='choose_next_task',
        python_callable=_choose_next_task,
        do_xcom_push = False
    )

    skipped_task = DummyOperator(
        task_id='skipped_task'
    )

    email_task = PythonOperator(
        task_id='email_task',
        python_callable=notify_email,
        provide_context=True,
        dag=dag,
    )

    delete_xcom_task = PostgresOperator(
        task_id='delete-xcom-task',
        postgres_conn_id='postgres_1',
        sql="delete from xcom where dag_id='"+dag.dag_id+"' and date(execution_date)=date('{{ ds }}')",
        trigger_rule='one_success',
        dag=dag
    )


    get_slots >> choose_next_task >> [skipped_task, email_task] >> delete_xcom_task
