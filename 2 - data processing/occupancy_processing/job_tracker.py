import datetime
import psycopg2
import configparser
import random
import logging
import logging.config
from psycopg2 import sql
from pathlib import Path


def create_job_table():
    """ insert a new vendor into the vendors table """
    sql = """CREATE TABLE spark_job (job_id INT NOT NULL, job_name VARCHAR(50),\
             status VARCHAR(50), dataset VARCHAR(50), loadtype VARCHAR(50), step INT,\
             stepdesc VARCHAR(50), year_processed VARCHAR(10),\date DATE NOT NULL)"""

    try:
        conn = get_db_connection()
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement                                
        cur.execute(sql)
       
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def insert_job_details(job_id, job_name, status, dataset, loadtype, step, stepdesc, year_processed, date):
    """ insert job details into the table """
    sql = """insert into spark_job (job_id, job_name, status, dataset, loadtype, step, stepdesc,year_processed, date)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
    conn = None
    vendor_id = None
    try:
        conn = get_db_connection()
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (job_id,job_name,status, dataset,loadtype, step, stepdesc, year_processed, date))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def assign_job_id():
    
    job_id = random.randint(1,10)
    return job_id

def get_job_status(job_id):
# connect db and send sql query
    table_name = "spark_job"
    sql ="""SELECT spark_job.status FROM spark_job where spark_job.job_id= %s ;"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        print(" Fetching status of the job from the table")
        cursor.execute(sql, (job_id,))
        status = cursor.fetchone()[0]
        return status
    except (Exception, psycopg2.Error) as error:
        print("Error getting value from the Database {}".format(error))
        return

def get_historic_job_status(year):
# connect db and send sql query
    table_name = "spark_job"
    sql ="""SELECT spark_job.status FROM spark_job where spark_job.year_processed= %s ;"""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        print(" Fetching status of the job from the table")
        cursor.execute(sql, (year,))
        status = cursor.fetchone()[0]
        return status
    except (Exception, psycopg2.Error) as error:
        print("Error getting value from the Database {}".format(error))
        return



def get_db_connection():
    connection = None

    # Construct connection string
    host = ""
    user = ""
    dbname = ""
    password = ""
    sslmode = ""
    
    try:
        connection = psycopg2.connect(user='postgres',
                                      password='Quark@2416',
                                      host='127.0.0.1',
                                      port='5432',
                                      database='postgres_db')
        print(" Successfully connected to postgres DB")
    except (Exception, psycopg2.Error) as error:
        logging.error("Error while connecting to PostgreSQL {}".format(error))
        
    return connection