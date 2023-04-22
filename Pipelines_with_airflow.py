from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Define the DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract():
    # Extract the 3 files
     customer_data = pd.read_csv('/Users/ubogalugalu/airflow/data/customer_data.csv')
     order_data = pd.read_csv('/Users/ubogalugalu/airflow/data/order_data.csv')
     payment_data = pd.read_csv('/Users/ubogalugalu/airflow/data/payment_data.csv')

    # Transform data
    # convert date fields to the correct format using pd.to_datetime
     payment_data['payment_date'] = pd.to_datetime(payment_data['payment_date'])
     customer_data['date_of_birth'] = pd.to_datetime(customer_data['date_of_birth'])
     order_data['order_date']= pd.to_datetime(order_data['order_date'])

    # merge customer and order dataframes on the customer_id column
     merged_data = customer_data.merge(order_data, how= 'inner', on = 'customer_id')

    # merge payment dataframe with the merged dataframe on the order_id and customer_id columns
     final_df = merged_data.merge(payment_data, how = 'inner', on=['order_id','customer_id'])
     final_df_path = '/Users/ubogalugalu/airflow/data/extracted.csv'
     final_df.to_csv(final_df_path, index= True)

def transform():
    final_df = pd.read_csv('/Users/ubogalugalu/airflow/data/extracted.csv')
    # drop unnecessary columns like customer_id and order_id
    final_df.drop(columns=['customer_id','order_id'],inplace= True)

    # group the data by customer and aggregate the amount paid using sum
    final_df=final_df.groupby(by = ['first_name','last_name']).sum('amount')

    # create a new column to calculate the total value of orders made by each customer
    final_df['total_value'] = final_df['amount']*final_df['price']

    # calculate the customer lifetime value using the formula CLV = (average order value) x (number of orders made per year) x (average customer lifespan) 
    final_df['life_time_value'] = round((final_df['total_value'].mean()*final_df['amount']*6),2)
     
    transformed_filepath = '/Users/ubogalugalu/airflow/data/my_transformed_file.csv'
    final_df.to_csv(transformed_filepath, index=True)

def load_dataframe_to_postgres():
    # Load the dataframe
    df = pd.read_csv( '/Users/ubogalugalu/airflow/data/my_transformed_file.csv')
    # Connect to the database
    engine = create_engine('postgresql://postgres:ubo123@34.173.130.172:5432/postgres')
   
    
    # Write the dataframe to the database
    df.to_sql('my_table', engine, if_exists='replace', index=False)

# Define the PythonOperator to load the dataframe to Postgres

with DAG('Extract_Transform_Load', default_args=default_args, schedule_interval='* * 5 * *') as dag:
        task_1 = PythonOperator(
            task_id='Extract',
            python_callable=extract,
            dag=dag
        )
        task_2 = PythonOperator(
            task_id='Transform',
            python_callable=extract,
            dag=dag
        )
        task_3 = PythonOperator(
            task_id='Load',
            python_callable = load_dataframe_to_postgres,
            dag=dag
        )



# Define the order of tasks in the DAG
task_1 >> task_2 >> task_3
