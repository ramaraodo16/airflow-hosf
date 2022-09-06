from airflow import DAG
import airflow
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import wget
import shutil

from datetime import datetime,timedelta
import os
import zipfile

args = {"owner":"Airflow", "start_date":datetime(2022,7,30)}

dags = DAG(dag_id="nse_daily_data_load",default_args =args,
schedule_interval= '37 13 * * *')

intr_stg_stg_load_query ="""copy into "EDW_DEV"."EDW"."NSE_STOCK_DAILY_DATA_STG" 
from @EDW_DEV.EDW.NSE_DAILY_STG/ pattern =  '.*cm.*' file_format = 'EDW_DEV.EDW.NSE_SD_FMT_CSV' purge =true; """

stg_to_tgt_load = """ MERGE into EDW_DEV.EDW.NSE_STOCK_DAILY_DATA_F f1
USING EDW_DEV.EDW.NSE_STOCK_DAILY_DATA_STream stg1
on f1.SYMBOL = stg1.SYMBOL and f1.TIMESTAMP = stg1.TIMESTAMP 
when not matched and stg1.METADATA$ACTION ='INSERT' and stg1.METADATA$ISUPDATE = FALSE 
then
insert (f1.SYMBOL,f1.SERIES,f1.OPEN,f1.HIGH,f1.LOW,f1.CLOSE,f1.LAST,f1.PREVCLOSE,f1.TOTTRDQTY,f1.TOTTRDVAL,f1.TIMESTAMP,f1.TOTALTRADES,f1.ISIN)  
values (stg1.SYMBOL,stg1.SERIES,stg1.OPEN,stg1.HIGH,stg1.LOW,stg1.CLOSE,stg1.LAST,stg1.PREVCLOSE,stg1.TOTTRDQTY,stg1.TOTTRDVAL,stg1.TIMESTAMP,stg1.TOTALTRADES,stg1.ISIN);
"""
tru_rload = """ call EDW_DEV.EDW.LOAD_NSE_DAILY_DATA_WITH_MEASURES(); """
def cleat_curr_dir_files():
    src = '/home/osboxes/airflow/'
    tgt = '/home/osboxes/Desktop/bkp/'
    dt_name = datetime.now().date().strftime('%Y-%m-%d')
    #pat1 =pat1+'/'
    sub_fol = dt_name
    try:
        ntgt = os.path.join(tgt,sub_fol)
        os.mkdir(ntgt)
    except:
        print("directory already available")
        print("directory already available")
        
        tgt_f= tgt+sub_fol+'/'
        
        get_sub_dir = os.listdir(tgt_f)
        print("get sub dir",get_sub_dir)
        sub_t = 'c'+str(len(get_sub_dir)-1)
        print(tgt_f,sub_t)
        ntgt = os.path.join(tgt_f,sub_t)
        os.mkdir(ntgt)
        print("new tgt",ntgt)
    print(dt_name)
    get_cur = os.listdir()
    print(get_cur,"get current directory")
    get_path = os.path
    get_cur_path = os.path.dirname(os.path.abspath(__file__))
    pat1=get_cur_path+'/'
    print("Testin -------------",get_path,"-------",get_cur_path)
    for f in range(0,len(get_cur)):
        if ( get_cur[f].endswith('.zip')):
            print(get_cur[f],"file1 ======",get_cur_path)
            try:
                shutil.move(src+get_cur[f], ntgt)
            except Exception as error:
            
                print(pat1+get_cur[f],"\n error identified",error)
def load_local_inter_stg():
    ld_lcl_ins= SnowflakeHook(snowflake_conn_id="snowflake_conn_id")
    sql_t = (f"put file:///home/osboxes/Desktop/Files/cm*.csv @EDW_DEV.EDW.NSE_DAILY_STG; ")
    res1 = ld_lcl_ins.run(sql = sql_t)
def nse_data_down_site():
    dt = datetime.now().date()
    
    dnum = dt.day
    mname = dt.strftime("%b").upper()
    mnum = dt.month
    year = dt.year
    ymdnum = year*10000+(mnum*100)+dnum
    ymnum= year*100+mnum
    stdnum = 20220825

    files = []
    #files1 = []
    for k in range(2020,2023):
        #print(k)
        
        #monts= ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC']
        #montnum = [1,2,3,4,5,6,7,8,9,10,11,12]
        monkey_it = {'JAN' : 1,'FEB': 2,'MAR': 3,'APR': 4,'MAY': 5,'JUN': 6,
                    'JUL': 7,'AUG': 8,'SEP': 9,'OCT': 10,'NOV': 11,'DEC' : 12}
        if (k <= year ):
                pass
        else:
                break
        for key,val in monkey_it.items():
            if (ymnum >= k*100+val):
                pass
            else:
                break
            for i in range(1,31):
                if ((ymdnum >= k*10000+val*100+i) & (stdnum <= k*10000+val*100+i)):
                    
            
                    filename= 'cm'+str(i).zfill(2)+key+str(k)+'bhav'
                    print('\n'+filename)
                    
                    url = 'https://www1.nseindia.com/content/historical/EQUITIES/'+str(k)+'/'+key+'/'+filename+'.csv.zip'
                    #print(url)
                    try:
                        print("enter try in loop")
                        print(url)
                        wget.download(url)
                        print(url)
                        files.append(filename+'.csv.zip')
                        #files.append(filename+'.csv')
                    except:
                        print("issue occured")
                else:
                    if(ymdnum >= k*10000+val*100+i):
                        pass
                    else:
                        break
        monkey_it.clear

    print(files)
        
    import zipfile

    for fw in files: 
        print(fw)
        with zipfile.ZipFile(fw, 'r') as zip_ref: 
            zip_ref.extractall('/home/osboxes/Desktop/Files')
        try:
            shutil.move(fw,'/home/osboxes/Desktop/Files')
        except:
            pass


def files_move_to_archives():
    
    src = '/home/osboxes/Desktop/Files/'
    tgt = '/home/osboxes/Desktop/archives_files/'
    dt_name = datetime.now().date().strftime('%Y-%m-%d')
    print(dt_name)
    sub_fol = dt_name

    allfiels = os.listdir(src)
    try:
        ntgt = os.path.join(tgt,sub_fol)
        os.mkdir(ntgt)
    except:
        print("directory already available")
    print(ntgt,"target file location")  
    for f1 in allfiels:
        try:
            shutil.move(src+f1, ntgt)
        except Exception as error:
            get_sub_dir = os.listdir(tgt)
            
            sub_t = sub_fol+'_'+str(len(get_sub_dir))
            ntgt = os.path.join(tgt,sub_t)
            os.mkdir(ntgt)  
            
            print(src+f1,"\n error identified",error)
with dags:
    files_clear =PythonOperator(
        task_id = "old_files_clear",
        python_callable= cleat_curr_dir_files
    )
    file_archive =PythonOperator(
        task_id = "nse_files_move",
        python_callable= files_move_to_archives
    )
    nse_download =PythonOperator(
        task_id = "nse_download_files",
        python_callable= nse_data_down_site
    )


    load_ftp_internal = PythonOperator(
        task_id = "load_ftp_namedstage",
        python_callable=load_local_inter_stg   


    )
    load_to_stg = SnowflakeOperator(
        task_id = "Named_stg_copy_tstg",
        sql = intr_stg_stg_load_query,
        snowflake_conn_id = "snowflake_conn_id"
    )
    insert_load_tgt = SnowflakeOperator(

        task_id = "Stg_tgt_merge",
        sql = stg_to_tgt_load,
        snowflake_conn_id = "snowflake_conn_id"
    )
    trun_reload_sproc = SnowflakeOperator(
        task_id = "Truncate_relaod_table",
        sql= tru_rload,
        snowflake_conn_id = "snowflake_conn_id"

    )
    files_clear.set_downstream(file_archive)
    file_archive.set_downstream(nse_download)
    nse_download.set_downstream(load_ftp_internal)
    load_ftp_internal >> load_to_stg >> insert_load_tgt >> trun_reload_sproc
