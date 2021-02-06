from csv import reader
import logging
import pandas as pd
import os

logging.basicConfig(filename='scale.log', level=logging.INFO)
fileDir = os.path.dirname(os.path.realpath('__file__'))
# logger = logging.getLogger('Scale_marketing')


def row_format(file):
    clients_df = pd.read_csv('~/campaign_files_airflow/clients.csv')
    customers_df = pd.read_csv('~/campaign_files_airflow/customers.csv')
    campaigns_df = pd.read_csv('~/campaign_files_airflow/campaigns.csv')
    
    try:
        logging.info("Opening metrics file")
        opened_file = open(file)
        read_file = reader(opened_file)
        apps_data = list(read_file)
        opened_file.close()

        idx = 1
#         me_list = [['client_id', 'campaign_id', 'metric_date','metric_name','meric_value']]
        me_list = []
        metrics = apps_data[0][3:]
        while idx < len(apps_data):


            id_id_date = apps_data[idx][:3]
            l = id_id_date[:] # I will append to l, I don't want to change id_id_date
            value = apps_data[idx][3:] # Value changes but metrics does not
            for i in range(len(metrics)):

                l.append(metrics[i])
                l.append(float(value[i])) #changing value form string to float
                me_list.append(l)
                l = id_id_date[:]
                logging.info("%sth row is transformed to clomnar format" % idx)

            idx += 1

        df = pd.DataFrame(me_list)
        logging.info("Giving the dataframe column names . . .")
        df.columns =['client_id', 'campaign_id', 'metric_date','metric_name','metric_value']
        df["client_id"] = pd.to_numeric(df["client_id"])
        df["campaign_id"] = pd.to_numeric(df["campaign_id"])
        
        logging.info("merging all dataframes . . .")
        df.merge(customers_df,on='client_id')\
        .merge(clients_df,on='client_id')\
        .merge(campaigns_df,on=["client_id","campaign_id"]).to_csv("~/campaign_files_airflow/metrics_columnar.csv", index=False)
        
    except Exception as e:
        logging.error(e)

def preprocessing():
    # filename = os.path.join(fileDir, '../campaign_files/clients.csv')
    row_format(r"/usr/local/airflow/campaign_files_airflow/metrics.csv")



def derived_feat(col1, col2):
    if col2 ==0:
        return 0
    return col1/col2




def kpi_push(file):
    df = pd.read_csv(file)
    length = len(df)
    dayly_net_spend = length *[0]
    dayly_gross_spend = length *[0]
    imp000= length *[0]
    impressions = length *[0]
    clicks = length *[0]
    
    raw_kpi_dict = {"imp000":imp000,
                    "dayly_gross_spend":dayly_gross_spend,
                    "dayly_net_spend":dayly_net_spend,
                    "impressions":impressions,
                    "clicks":clicks}
    
    
    var_name = df['metric_name'].tolist()
    var_value = df['metric_value'].tolist()
        
    for num, ele in enumerate(var_name):
        if ele == 'cost_micros':
            dayly_net_spend[num] = var_value[num]* 10.0E-7
            dayly_gross_spend[num] = dayly_net_spend[num]/(1.0-0.26)

        elif ele == 'impressions':
            impressions[num] = var_value[num] # I need a list for all imprssions too
#               imp000[num] = var_value[num]/ 1000.0
            imp000[num] = impressions[num]/ 1000.0 


        elif ele == 'clicks':
            clicks[num] = var_value[num]
            

    #adding the kpi coulmns t the dataframe
    for k, value in raw_kpi_dict.items():
        df[k]= value
    
    
    #Derived kpi features
    df['CTR'] = df.apply(lambda row: derived_feat(row['clicks'],row['impressions']),axis=1)
    df['eCPM'] = df.apply(lambda row: derived_feat(row['dayly_net_spend'],row['imp000']),axis=1)
    
    #Renaming the columns metrics_date
    df = df.rename(columns={'metric_date': 'date'})
    
   
    
    #Choosing the columns of interests
    df = df[['customer_id','client_id','date','imp000',
             'dayly_gross_spend','dayly_net_spend','impressions','clicks','CTR','eCPM']]
    
    #melting it for the short version in pandas                        
    df = df.melt(id_vars=['customer_id','client_id','date'])
    
    df = df.rename(columns={'variable': 'kpi'})
    
    df.to_parquet('~/campaign_files_airflow/scale_report.parquet', engine='pyarrow',partition_cols=['customer_id'])


def kpi_to_parquet():
    kpi_push(r"/usr/local/airflow/campaign_files_airflow/metrics_columnar.csv")


if __name__ == '__main__':
    preprocessing()


