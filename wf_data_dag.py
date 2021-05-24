from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

import boto3
from dbfread import DBF
import pandas as pd

import os 
from io import BytesIO, StringIO
from datetime import datetime, timedelta

# Configuring Spaces Client
session = boto3.session.Session()
client = session.client('s3',
                        region_name='nyc3',                                     # Replace with correct region
                        endpoint_url='https://nyc3.digitaloceanspaces.com',     # Replace with correct endpoint
                        aws_access_key_id=os.getenv('SPACES_KEY'),              # Replace with access key
                        aws_secret_access_key=os.getenv('SPACES_SECRET'))       # Replace with secret key

s3 = session.resource('s3',
                        region_name='nyc3',                                     # Replace with correct region
                        endpoint_url='https://nyc3.digitaloceanspaces.com',     # Replace with correct endpoint
                        aws_access_key_id=os.getenv('SPACES_KEY'),              # Replace with access key
                        aws_secret_access_key=os.getenv('SPACES_SECRET'))       # Replace with secret key

def_args = {
    'owner':'airflow',
    'start_date': datetime(2021, 1, 1),
    'depends_on_past': False,
    'email': ['ivanedric@gmail.com'],   # Change to preferred email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=20)
    }

with DAG('wf_data_dag',
    default_args=def_args,
    schedule_interval='@weekly',        # '0 0 * * 0': Runs weekly on Sunday mornings 
    catchup=False,) as dag:
    
    def _DBF_to_Sales_Reports(**kwargs):
        ''' Converts DBFs to CSV and uploads back to Ocean Spaces '''
        # Read from DigitalOcean Space
        sdet_obj = client.get_object(kwargs['Bucket'], kwargs['Key_SDET'])
        sdet_dbf = DBF(BytesIO(sdet_obj['Body'].read()))
        sdet_df = pd.DataFrame(iter(sdet_dbf))

        sls_obj = client.get_object(kwargs['Bucket'], kwargs['Key_SLS'])
        sls_dbf = DBF(BytesIO(sls_obj['Body'].read()))
        sls_df = pd.DataFrame(iter(sls_dbf))

        pages_obj = client.get_object(kwargs['Bucket'], kwargs['Key_PAGES'])
        pages_dbf = DBF(BytesIO(pages_obj['Body'].read()), encoding = "ISO-8859-1")
        pages_df = pd.DataFrame(iter(pages_dbf))

        menu_obj = client.get_object(kwargs['Bucket'], kwargs['Key_MENU'])
        menu_dbf = DBF(BytesIO(menu_obj['Body'].read()), encoding = "ISO-8859-1")
        menu_df = pd.DataFrame(iter(menu_dbf))

        pagetype_obj = client.get_object(kwargs['Bucket'], kwargs['Key_PAGETYPE'])
        pagetype_dbf = DBF(BytesIO(pagetype_obj['Body'].read()))
        pagetype_df = pd.DataFrame(iter(pagetype_dbf))

        revcent_obj = client.get_object(kwargs['Bucket'], kwargs['Key_REVCENT'])
        revcent_dbf = DBF(BytesIO(revcent_obj['Body'].read()))
        revcent_df = pd.DataFrame(iter(revcent_dbf))

        tipopag_obj = client.get_object(kwargs['Bucket'], kwargs['Key_TIPOPAG'])
        tipopag_dbf = DBF(BytesIO(tipopag_obj['Body'].read()))
        tipopag_df = pd.DataFrame(iter(tipopag_dbf))

        # Write back to DigitalOcean Space
        sdet_csv_buffer = StringIO()
        sdet_df.to_csv(sdet_csv_buffer)
        sdet_key = kwargs['Target_Key_SDET'] + '/SDET_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'               # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], sdet_key, Body=sdet_csv_buffer.getvalue())

        sls_csv_buffer = StringIO()
        sls_df.to_csv(sls_csv_buffer)
        sls_key = kwargs['Target_Key_SLS'] + '/SLS_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'                  # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], sls_key, Body=sls_csv_buffer.getvalue())

        pages_csv_buffer = StringIO()
        pages_df.to_csv(pages_csv_buffer)
        pages_key = kwargs['Target_Key_PAGES'] + '/PAGES_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'            # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], pages_key, Body=pages_csv_buffer.getvalue())

        menu_csv_buffer = StringIO()
        menu_df.to_csv(menu_csv_buffer)
        menu_key = kwargs['Target_Key_MENU'] + '/MENU_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'               # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], menu_key, Body=menu_csv_buffer.getvalue())

        pagetype_csv_buffer = StringIO()
        pagetype_df.to_csv(pagetype_csv_buffer)
        pagetype_key = kwargs['Target_Key_PAGETYPE'] + '/PAGETYPE_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'   # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], pagetype_key, Body=pagetype_csv_buffer.getvalue())

        revcent_csv_buffer = StringIO()
        revcent_df.to_csv(revcent_csv_buffer)
        revcent_key = kwargs['Target_Key_REVCENT'] + '/REVCENT_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'      # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], revcent_key, Body=revcent_csv_buffer.getvalue())

        tipopag_csv_buffer = StringIO()
        tipopag_df.to_csv(tipopag_csv_buffer)
        tipopag_key = kwargs['Target_Key_TIPOPAG'] + '/TIPOPAG_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'      # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], tipopag_key, Body=tipopag_csv_buffer.getvalue())

        ''' We  can also delete the original DBF from Digital Ocean to save costs '''
        # client.delete_objects(
        #     Bucket=kwargs['Bucket'],
        #     Delete={
        #         'Objects': [
        #             {'Key': kwargs['Key_SDET']},
        #             {'Key': kwargs['Key_SLS']},
        #             {'Key': kwargs['Key_PAGES']},
        #             {'Key': kwargs['Key_MENU']},
        #             {'Key': kwargs['Key_PAGETYPE']},
        #             {'Key': kwargs['Key_REVCENT']},
        #             {'Key': kwargs['Key_TIPOPAG']}
        #         ]
        #     }
        # )

        ''' Generating reports '''
        grp_lookup_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_GROUP_LOOKUP'])
        grp_lookup_file = pd.read_excel(grp_lookup_obj['Body'], index_col='Unnamed: 0')
        rc_lookup_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_RC_LOOKUP'])
        rc_lookup_file = pd.read_excel(rc_lookup_obj['Body'], index_col='Unnamed: 0')

        # Group Sales
        group_sales_sdet = sdet_df[['ORD_DATE', 'REF_NO']]
        group_sales_sdet['SALES_VAT'] = sdet_df['RAW_PRICE'] - sdet_df['VAT_ADJ']
        group_sales_df = pd.merge(group_sales_sdet, menu_df, how="left", on=["REF_NO"])                             # Joining the MENU table with the SDET table
        group_sales_df = group_sales_df[['ORD_DATE', 'REF_NO', 'DESCRIPT', 'PAGE_NUM', 'SALES_VAT']]                # Only taking these columns
        group_sales_df = pd.merge(group_sales_df, pages_df, how='left', on=['PAGE_NUM'])                            # Joining MENU + SDET with PAGES
        group_sales_df = group_sales_df[['ORD_DATE', 'REF_NO', 'DESCRIPT', 'PAGE_NUM', 'PAGE_NAME', 'SALES_VAT']]   # Only taking these columns
        group_sales_df['Branch'] = kwargs['Branch']

        # Revenue Center Sales
        rc_sales_sls = sls_df[['DATE', 'BILL_NO', 'RECEIVED', 'REV_CENTER', 'TAXES']]                               
        rc_sales_df = pd.merge(rc_sales_sls, revcent_df, how="left", on=["REV_CENTER"])                             # Merging SLS and REVCENT
        rc_sales_df = rc_sales_df[['DATE', 'BILL_NO', 'REV_CENTER', 'RC_NAME', 'RECEIVED', 'TAXES']]
        rc_sales_df['Branch'] = kwargs['Branch']
        
        # Item Sales by Group
        sdet_menu = pd.merge(sdet_df, menu_df, on ='REF_NO',how ='left')
        sdet_menu_cut = sdet_menu[["ORD_DATE","REF_NO","DESCRIPT","PAGE_NUM","RAW_PRICE","VAT_ADJ"]]
        item_sales_long = pd.merge(sdet_menu_cut, pages_df, on ='PAGE_NUM',how ='left')
        item_sales = item_sales_long[["ORD_DATE", "REF_NO","DESCRIPT","PAGE_NUM","PAGE_NAME","RAW_PRICE","VAT_ADJ"]]
        item_sales["Sales + VAT"] = item_sales["RAW_PRICE"] - item_sales["VAT_ADJ"]
        item_sales.rename(columns={'PAGE_NAME': 'Group', 'PAGE_NUM': 'Group Number'}, inplace=True)
        pdf_grp_df = item_sales.groupby(["Group Number","Group"]).agg(
            Sales=pd.NamedAgg(column="RAW_PRICE", aggfunc="sum"),
            Taxes=pd.NamedAgg(column="VAT_ADJ", aggfunc="sum"),
            Total = pd.NamedAgg(column="Sales + VAT", aggfunc="sum"))
        pdf_grp_df['Branch'] = kwargs['Branch']
        
        # Item Sales by Revenue Center
        pdf_rc_df = rc_sales_df.groupby(["REV_CENTER", "RC_NAME"]).agg(
            Trans=pd.NamedAgg(column="REV_CENTER", aggfunc="count"),
            Cust=pd.NamedAgg(column="REV_CENTER", aggfunc="count"),
            Sales=pd.NamedAgg(column="RECEIVED", aggfunc="sum"),
            Taxes=pd.NamedAgg(column="TAXES", aggfunc="sum"))
        pdf_rc_df['Total'] = pdf_rc_df['Taxes'] + pdf_rc_df['Sales']
        pdf_rc_df['Trans Average'] = pdf_rc_df['Sales'] / pdf_rc_df['Trans']
        pdf_rc_df['Customer Average'] = pdf_rc_df['Sales'] / pdf_rc_df['Cust']
        pdf_rc_df['Branch'] = kwargs['Branch']

        # VLOOKUP
        grp_lookup_file.columns = ["PAGE_NAME", "Simple group"]
        rc_lookup_file.drop(rc_lookup_file.columns[[2,3,4,5,6,7]], axis = 1, inplace = True)
        rc_lookup_file.columns = ["RC_NAME", "Simple revcent"]
        vlookup_rc = pd.merge(pdf_rc_df, rc_lookup_file, on ='RC_NAME',how ='left')                                 # Merge Revenue Center with Sales Report
        sumif_rc = vlookup_rc.groupby("Simple revcent")["Total"].sum()                                              # Get the sum of sales per Revenue Center
        sumif_rc = sumif_rc.to_frame()
        sumif_rc['Branch'] = kwargs['Branch']

        vlookup_grp = pd.merge(group_sales_df, grp_lookup_file, on ='PAGE_NAME',how ='left')                        # Merge Group lookup file with Sales Report
        sumif_grp = vlookup_grp.groupby("Simple group")["SALES_VAT"].sum()                                          # Get brand sales
        sumif_grp = sumif_grp.to_frame()
        sumif_grp['Branch'] = kwargs['Branch']

        # Send back to DigitalOcean Space
        group_sales_csv_buffer = StringIO()
        group_sales_df.to_csv(group_sales_csv_buffer)
        group_sales_key = kwargs['Target_Key_GROUP_SALES'] + '/GROUP_SALES_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'               # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], group_sales_key, Body=group_sales_csv_buffer.getvalue())

        rc_sales_csv_buffer = StringIO()
        rc_sales_df.to_csv(rc_sales_csv_buffer)
        rc_sales_key = kwargs['Target_Key_RC_SALES'] + '/RC_SALES_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'                  # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], rc_sales_key, Body=rc_sales_csv_buffer.getvalue())

        pdf_grp_csv_buffer = StringIO()
        pdf_grp_df.to_csv(pdf_grp_csv_buffer)
        item_sales_group_key = kwargs['Target_Key_ITEM_SALES_GROUP'] + '/ITEM_SALES_GROUP_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'            # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], item_sales_group_key, Body=pdf_grp_csv_buffer.getvalue())

        pdf_rc_csv_buffer = StringIO()
        pdf_rc_df.to_csv(pdf_rc_csv_buffer)
        item_sales_rc_key = kwargs['Target_Key_ITEM_SALES_RC'] + '/ITEM_SALES_RC_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'               # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], item_sales_rc_key, Body=pdf_rc_csv_buffer.getvalue())

        sumif_rc_csv_buffer = StringIO()
        sumif_rc.to_csv(sumif_rc_csv_buffer)
        sumif_rc_key = kwargs['Target_Key_SUMIF_RC'] + '/SUMIF_RC_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'   # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], sumif_rc_key, Body=sumif_rc_csv_buffer.getvalue())

        sumif_grp_csv_buffer = StringIO()
        sumif_grp.to_csv(sumif_grp_csv_buffer)
        sumif_grp_key = kwargs['Target_Key_SUMIF_GRP'] + '/SUMIF_GRP_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'      # NOTE: Edit this to fit desired bucket structure
        client.put_object(kwargs['Bucket'], sumif_grp_key, Body=sumif_grp_csv_buffer.getvalue())
    
    # def _generate_reports(**kwargs):
        # ''' Generates needed reports for one branch'''
        # # Get CSVs from DigitalOcean Space
        # sdet_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_SDET'])
        # sdet_df = pd.read_csv(sdet_obj['Body'], index_col='Unnamed: 0')
        # sls_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_SLS'])
        # sls_df = pd.read_csv(sls_obj['Body'], index_col='Unnamed: 0')
        # pages_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_PAGES'])
        # pages_df = pd.read_csv(pages_obj['Body'], index_col='Unnamed: 0')
        # menu_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_MENU'])
        # menu_df = pd.read_csv(menu_obj['Body'], index_col='Unnamed: 0')
        # revcent_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_REVCENT'])
        # revcent_df = pd.read_csv(revcent_obj['Body'], index_col='Unnamed: 0')
        # # grp_lookup_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_GROUP_LOOKUP'])
        # grp_lookup_file = pd.read_excel(grp_lookup_obj['Body'], index_col='Unnamed: 0')
        # rc_lookup_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_RC_LOOKUP'])
        # rc_lookup_file = pd.read_excel(rc_lookup_obj['Body'], index_col='Unnamed: 0')

        # # Group Sales
        # group_sales_sdet = sdet_df[['ORD_DATE', 'REF_NO']]
        # group_sales_sdet['SALES_VAT'] = sdet_df['RAW_PRICE'] - sdet_df['VAT_ADJ']
        # group_sales_df = pd.merge(group_sales_sdet, menu_df, how="left", on=["REF_NO"])                             # Joining the MENU table with the SDET table
        # group_sales_df = group_sales_df[['ORD_DATE', 'REF_NO', 'DESCRIPT', 'PAGE_NUM', 'SALES_VAT']]                # Only taking these columns
        # group_sales_df = pd.merge(group_sales_df, pages_df, how='left', on=['PAGE_NUM'])                            # Joining MENU + SDET with PAGES
        # group_sales_df = group_sales_df[['ORD_DATE', 'REF_NO', 'DESCRIPT', 'PAGE_NUM', 'PAGE_NAME', 'SALES_VAT']]   # Only taking these columns
        # group_sales_df['Branch'] = kwargs['Branch']

        # # Revenue Center Sales
        # rc_sales_sls = sls_df[['DATE', 'BILL_NO', 'RECEIVED', 'REV_CENTER', 'TAXES']]                               
        # rc_sales_df = pd.merge(rc_sales_sls, revcent_df, how="left", on=["REV_CENTER"])                             # Merging SLS and REVCENT
        # rc_sales_df = rc_sales_df[['DATE', 'BILL_NO', 'REV_CENTER', 'RC_NAME', 'RECEIVED', 'TAXES']]
        # rc_sales_df['Branch'] = kwargs['Branch']
        
        # # Item Sales by Group
        # sdet_menu = pd.merge(sdet_df, menu_df, on ='REF_NO',how ='left')
        # sdet_menu_cut = sdet_menu[["ORD_DATE","REF_NO","DESCRIPT","PAGE_NUM","RAW_PRICE","VAT_ADJ"]]
        # item_sales_long = pd.merge(sdet_menu_cut, pages_df, on ='PAGE_NUM',how ='left')
        # item_sales = item_sales_long[["ORD_DATE", "REF_NO","DESCRIPT","PAGE_NUM","PAGE_NAME","RAW_PRICE","VAT_ADJ"]]
        # item_sales["Sales + VAT"] = item_sales["RAW_PRICE"] - item_sales["VAT_ADJ"]
        # item_sales.rename(columns={'PAGE_NAME': 'Group', 'PAGE_NUM': 'Group Number'}, inplace=True)
        # pdf_grp_df = item_sales.groupby(["Group Number","Group"]).agg(
        #     Sales=pd.NamedAgg(column="RAW_PRICE", aggfunc="sum"),
        #     Taxes=pd.NamedAgg(column="VAT_ADJ", aggfunc="sum"),
        #     Total = pd.NamedAgg(column="Sales + VAT", aggfunc="sum"))
        # pdf_grp_df['Branch'] = kwargs['Branch']
        
        # # Item Sales by Revenue Center
        # pdf_rc_df = rc_sales_df.groupby(["REV_CENTER", "RC_NAME"]).agg(
        #     Trans=pd.NamedAgg(column="REV_CENTER", aggfunc="count"),
        #     Cust=pd.NamedAgg(column="REV_CENTER", aggfunc="count"),
        #     Sales=pd.NamedAgg(column="RECEIVED", aggfunc="sum"),
        #     Taxes=pd.NamedAgg(column="TAXES", aggfunc="sum"))
        # pdf_rc_df['Total'] = pdf_rc_df['Taxes'] + pdf_rc_df['Sales']
        # pdf_rc_df['Trans Average'] = pdf_rc_df['Sales'] / pdf_rc_df['Trans']
        # pdf_rc_df['Customer Average'] = pdf_rc_df['Sales'] / pdf_rc_df['Cust']
        # pdf_rc_df['Branch'] = kwargs['Branch']

        # # VLOOKUP
        # grp_lookup_file.columns = ["PAGE_NAME", "Simple group"]
        # rc_lookup_file.drop(rc_lookup_file.columns[[2,3,4,5,6,7]], axis = 1, inplace = True)
        # rc_lookup_file.columns = ["RC_NAME", "Simple revcent"]
        # vlookup_rc = pd.merge(pdf_rc_df, rc_lookup_file, on ='RC_NAME',how ='left')                                 # Merge Revenue Center with Sales Report
        # sumif_rc = vlookup_rc.groupby("Simple revcent")["Total"].sum()                                              # Get the sum of sales per Revenue Center
        # sumif_rc = sumif_rc.to_frame()
        # sumif_rc['Branch'] = kwargs['Branch']

        # vlookup_grp = pd.merge(group_sales_df, grp_lookup_file, on ='PAGE_NAME',how ='left')                        # Merge Group lookup file with Sales Report
        # sumif_grp = vlookup_grp.groupby("Simple group")["SALES_VAT"].sum()                                          # Get brand sales
        # sumif_grp = sumif_grp.to_frame()
        # sumif_grp['Branch'] = kwargs['Branch']

        # # Send back to DigitalOcean Space
        # group_sales_csv_buffer = StringIO()
        # group_sales_df.to_csv(group_sales_csv_buffer)
        # group_sales_key = kwargs['Target_Key_GROUP_SALES'] + '/GROUP_SALES_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'               # NOTE: Edit this to fit desired bucket structure
        # client.put_object(kwargs['Bucket'], group_sales_key, Body=group_sales_csv_buffer.getvalue())

        # rc_sales_csv_buffer = StringIO()
        # rc_sales_df.to_csv(rc_sales_csv_buffer)
        # rc_sales_key = kwargs['Target_Key_RC_SALES'] + '/RC_SALES_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'                  # NOTE: Edit this to fit desired bucket structure
        # client.put_object(kwargs['Bucket'], rc_sales_key, Body=rc_sales_csv_buffer.getvalue())

        # pdf_grp_csv_buffer = StringIO()
        # pdf_grp_df.to_csv(pdf_grp_csv_buffer)
        # item_sales_group_key = kwargs['Target_Key_ITEM_SALES_GROUP'] + '/ITEM_SALES_GROUP_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'            # NOTE: Edit this to fit desired bucket structure
        # client.put_object(kwargs['Bucket'], item_sales_group_key, Body=pdf_grp_csv_buffer.getvalue())

        # pdf_rc_csv_buffer = StringIO()
        # pdf_rc_df.to_csv(pdf_rc_csv_buffer)
        # item_sales_rc_key = kwargs['Target_Key_ITEM_SALES_RC'] + '/ITEM_SALES_RC_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'               # NOTE: Edit this to fit desired bucket structure
        # client.put_object(kwargs['Bucket'], item_sales_rc_key, Body=pdf_rc_csv_buffer.getvalue())

        # sumif_rc_csv_buffer = StringIO()
        # sumif_rc.to_csv(sumif_rc_csv_buffer)
        # sumif_rc_key = kwargs['Target_Key_SUMIF_RC'] + '/SUMIF_RC_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'   # NOTE: Edit this to fit desired bucket structure
        # client.put_object(kwargs['Bucket'], sumif_rc_key, Body=sumif_rc_csv_buffer.getvalue())

        # sumif_grp_csv_buffer = StringIO()
        # sumif_grp.to_csv(sumif_grp_csv_buffer)
        # sumif_grp_key = kwargs['Target_Key_SUMIF_GRP'] + '/SUMIF_GRP_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'      # NOTE: Edit this to fit desired bucket structure
        # client.put_object(kwargs['Bucket'], sumif_grp_key, Body=sumif_grp_csv_buffer.getvalue())

    def _merge_reports(**kwargs):
        ''' Merges reports '''
        # TODO: Finish concatenation of branch files
        # Read all branches from DigitalOcean Space
        try:
            # Merging for the item sales by group
            lf_bgc_isg_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_LF_BGC_ISG'])
            lf_bgc_isg_df = pd.read_csv(lf_bgc_isg_obj['Body'])
            wf_bgc_isg_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_WF_BGC_ISG'])
            wf_bgc_isg_df = pd.read_csv(wf_bgc_isg_obj['Body'])
            wf_greenhills_isg_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_WF_GREENHILLS_ISG'])
            wf_greenhills_isg_df = pd.read_csv(wf_greenhills_isg_obj['Body'])
            wf_podium_isg_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_WF_PODIUM_ISG'])
            wf_podium_isg_df = pd.read_csv(wf_podium_isg_obj['Body'])
            wf_rada_isg_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_WF_RADA_ISG'])
            wf_rada_isg_df = pd.read_csv(wf_rada_isg_obj['Body'])
            wf_rockwell_isg_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_WF_ROCKWELL_ISG'])
            wf_rockwell_isg_df = pd.read_csv(wf_rockwell_isg_obj['Body'])
            wf_salcedo_isg_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_WF_SALCEDO_ISG'])
            wf_salcedo_isg_df = pd.read_csv(wf_salcedo_isg_obj['Body'])
            wf_uptown_isg_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_WF_UPTOWN_ISG'])
            wf_uptown_isg_df = pd.read_csv(wf_uptown_isg_obj['Body'])
            wfi_bgc_isg_obj = s3.get_object(Bucket=kwargs['Bucket'], Key=kwargs['Key_WFI_BGC_ISG'])
            wfi_bgc_isg_df = pd.read_csv(wfi_bgc_isg_obj['Body'])
            # Concat files
            isg_frames = [lf_bgc_isg_df, wf_bgc_isg_df, wf_greenhills_isg_df, wf_podium_isg_df, wf_rada_isg_df, wf_rockwell_isg_df, wf_salcedo_isg_df, wf_uptown_isg_df, wfi_bgc_isg_df]
            isg_master_df = pd.concat(isg_frames)
            # Upload files back to DigitalOcean
            isg_master_csv_buffer = StringIO()
            isg_master_df.to_csv(isg_master_csv_buffer)
            isg_key = kwargs['Target_Key_ISG'] + '/ISG_Master_' + str(datetime.today().strftime('%Y-%m-%d')) + '.csv'      # NOTE: Edit this to fit desired bucket structure
            client.put_object(kwargs['Bucket'], isg_key, Body=isg_master_csv_buffer.getvalue())
            return success
        except:
            return failure

    # Jobs
    # NOTE: Must replace Buckets and Keys with correct Buckets + Keys
    LF_BGC_DBF_to_Sales_Reports = PythonOperator(
        task_id = 'LF_BGC_DBF_to_Sales_Reports',
        python_callable = _DBF_to_Sales_Reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_SDET': 'name_of_key',
                   'Key_SLS': 'name_of_key',
                   'Key_PAGES': 'name_of_key',
                   'Key_MENU': 'name_of_key',
                   'Key_PAGETYPE': 'name_of_key',
                   'Key_REVCENT': 'name_of_key',
                   'Key_TIPOPAG': 'name_of_key',
                   'Target_Key_SDET': 'name_of_key',
                   'Target_Key_SLS': 'name_of_key',
                   'Target_Key_PAGES': 'name_of_key',
                   'Target_Key_MENU': 'name_of_key',
                   'Target_Key_PAGETYPE': 'name_of_key',
                   'Target_Key_REVCENT': 'name_of_key',
                   'Target_Key_TIPOPAG': 'name_of_key',
                   'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
                   'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
                   'Target_Key_GROUP_SALES': 'name_of_key',
                   'Target_Key_RC_SALES': 'name_of_key',
                   'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
                   'Target_Key_ITEM_SALES_RC': 'name_of_key',
                   'Target_Key_SUMIF_RC': 'name_of_key',
                   'Target_Key_SUMIF_GRP': 'name_of_key',
                   'Branch': 'LF BGC'
                   }
    )

    WF_BGC_DBF_to_Sales_Reports = PythonOperator(
        task_id = 'WF_BGC_DBF_to_Sales_Reports',
        python_callable = _DBF_to_Sales_Reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_SDET': 'name_of_key',
                   'Key_SLS': 'name_of_key',
                   'Key_PAGES': 'name_of_key',
                   'Key_MENU': 'name_of_key',
                   'Key_PAGETYPE': 'name_of_key',
                   'Key_REVCENT': 'name_of_key',
                   'Key_TIPOPAG': 'name_of_key',
                   'Target_Key_SDET': 'name_of_key',
                   'Target_Key_SLS': 'name_of_key',
                   'Target_Key_PAGES': 'name_of_key',
                   'Target_Key_MENU': 'name_of_key',
                   'Target_Key_PAGETYPE': 'name_of_key',
                   'Target_Key_REVCENT': 'name_of_key',
                   'Target_Key_TIPOPAG': 'name_of_key',
                   'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
                   'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
                   'Target_Key_GROUP_SALES': 'name_of_key',
                   'Target_Key_RC_SALES': 'name_of_key',
                   'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
                   'Target_Key_ITEM_SALES_RC': 'name_of_key',
                   'Target_Key_SUMIF_RC': 'name_of_key',
                   'Target_Key_SUMIF_GRP': 'name_of_key',
                   'Branch': 'WF BGC'},
    )

    WF_Greenhills_DBF_to_Sales_Reports = PythonOperator(
        task_id = 'WF_Greenhills_DBF_to_Sales_Reports',
        python_callable = _DBF_to_Sales_Reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_SDET': 'name_of_key',
                   'Key_SLS': 'name_of_key',
                   'Key_PAGES': 'name_of_key',
                   'Key_MENU': 'name_of_key',
                   'Key_PAGETYPE': 'name_of_key',
                   'Key_REVCENT': 'name_of_key',
                   'Key_TIPOPAG': 'name_of_key',
                   'Target_Key_SDET': 'name_of_key',
                   'Target_Key_SLS': 'name_of_key',
                   'Target_Key_PAGES': 'name_of_key',
                   'Target_Key_MENU': 'name_of_key',
                   'Target_Key_PAGETYPE': 'name_of_key',
                   'Target_Key_REVCENT': 'name_of_key',
                   'Target_Key_TIPOPAG': 'name_of_key',
                   'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
                   'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
                   'Target_Key_GROUP_SALES': 'name_of_key',
                   'Target_Key_RC_SALES': 'name_of_key',
                   'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
                   'Target_Key_ITEM_SALES_RC': 'name_of_key',
                   'Target_Key_SUMIF_RC': 'name_of_key',
                   'Target_Key_SUMIF_GRP': 'name_of_key',
                   'Branch': 'WF_Greenhills'}
    )

    WF_Podium_DBF_to_Sales_Reports = PythonOperator(
        task_id = 'WF_Podium_DBF_to_Sales_Reports',
        python_callable = _DBF_to_Sales_Reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_SDET': 'name_of_key',
                   'Key_SLS': 'name_of_key',
                   'Key_PAGES': 'name_of_key',
                   'Key_MENU': 'name_of_key',
                   'Key_PAGETYPE': 'name_of_key',
                   'Key_REVCENT': 'name_of_key',
                   'Key_TIPOPAG': 'name_of_key',
                   'Target_Key_SDET': 'name_of_key',
                   'Target_Key_SLS': 'name_of_key',
                   'Target_Key_PAGES': 'name_of_key',
                   'Target_Key_MENU': 'name_of_key',
                   'Target_Key_PAGETYPE': 'name_of_key',
                   'Target_Key_REVCENT': 'name_of_key',
                   'Target_Key_TIPOPAG': 'name_of_key',
                   'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
                   'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
                   'Target_Key_GROUP_SALES': 'name_of_key',
                   'Target_Key_RC_SALES': 'name_of_key',
                   'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
                   'Target_Key_ITEM_SALES_RC': 'name_of_key',
                   'Target_Key_SUMIF_RC': 'name_of_key',
                   'Target_Key_SUMIF_GRP': 'name_of_key',
                   'Branch': 'WF Podium'}
    )

    WF_Rada_DBF_to_Sales_Reports = PythonOperator(
        task_id = 'WF_Rada_DBF_to_Sales_Reports',
        python_callable = _DBF_to_Sales_Reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_SDET': 'name_of_key',
                   'Key_SLS': 'name_of_key',
                   'Key_PAGES': 'name_of_key',
                   'Key_MENU': 'name_of_key',
                   'Key_PAGETYPE': 'name_of_key',
                   'Key_REVCENT': 'name_of_key',
                   'Key_TIPOPAG': 'name_of_key',
                   'Target_Key_SDET': 'name_of_key',
                   'Target_Key_SLS': 'name_of_key',
                   'Target_Key_PAGES': 'name_of_key',
                   'Target_Key_MENU': 'name_of_key',
                   'Target_Key_PAGETYPE': 'name_of_key',
                   'Target_Key_REVCENT': 'name_of_key',
                   'Target_Key_TIPOPAG': 'name_of_key',
                   'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
                   'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
                   'Target_Key_GROUP_SALES': 'name_of_key',
                   'Target_Key_RC_SALES': 'name_of_key',
                   'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
                   'Target_Key_ITEM_SALES_RC': 'name_of_key',
                   'Target_Key_SUMIF_RC': 'name_of_key',
                   'Target_Key_SUMIF_GRP': 'name_of_key',
                   'Branch': 'WF Rada'}
    )

    WF_Rockwell_DBF_to_Sales_Reports = PythonOperator(
        task_id = 'WF_Rockwell_DBF_to_Sales_Reports',
        python_callable = _DBF_to_Sales_Reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_SDET': 'name_of_key',
                   'Key_SLS': 'name_of_key',
                   'Key_PAGES': 'name_of_key',
                   'Key_MENU': 'name_of_key',
                   'Key_PAGETYPE': 'name_of_key',
                   'Key_REVCENT': 'name_of_key',
                   'Key_TIPOPAG': 'name_of_key',
                   'Target_Key_SDET': 'name_of_key',
                   'Target_Key_SLS': 'name_of_key',
                   'Target_Key_PAGES': 'name_of_key',
                   'Target_Key_MENU': 'name_of_key',
                   'Target_Key_PAGETYPE': 'name_of_key',
                   'Target_Key_REVCENT': 'name_of_key',
                   'Target_Key_TIPOPAG': 'name_of_key',
                   'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
                   'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
                   'Target_Key_GROUP_SALES': 'name_of_key',
                   'Target_Key_RC_SALES': 'name_of_key',
                   'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
                   'Target_Key_ITEM_SALES_RC': 'name_of_key',
                   'Target_Key_SUMIF_RC': 'name_of_key',
                   'Target_Key_SUMIF_GRP': 'name_of_key',
                   'Branch': 'WF Rockwell'}
    )
    
    WF_Salcedo_DBF_to_Sales_Reports = PythonOperator(
        task_id = 'WF_Salcedo_DBF_to_Sales_Reports',
        python_callable = _DBF_to_Sales_Reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_SDET': 'name_of_key',
                   'Key_SLS': 'name_of_key',
                   'Key_PAGES': 'name_of_key',
                   'Key_MENU': 'name_of_key',
                   'Key_PAGETYPE': 'name_of_key',
                   'Key_REVCENT': 'name_of_key',
                   'Key_TIPOPAG': 'name_of_key',
                   'Target_Key_SDET': 'name_of_key',
                   'Target_Key_SLS': 'name_of_key',
                   'Target_Key_PAGES': 'name_of_key',
                   'Target_Key_MENU': 'name_of_key',
                   'Target_Key_PAGETYPE': 'name_of_key',
                   'Target_Key_REVCENT': 'name_of_key',
                   'Target_Key_TIPOPAG': 'name_of_key',
                   'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
                   'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
                   'Target_Key_GROUP_SALES': 'name_of_key',
                   'Target_Key_RC_SALES': 'name_of_key',
                   'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
                   'Target_Key_ITEM_SALES_RC': 'name_of_key',
                   'Target_Key_SUMIF_RC': 'name_of_key',
                   'Target_Key_SUMIF_GRP': 'name_of_key',
                   'Branch': 'WF Salcedo'}
    )

    WF_Uptown_DBF_to_Sales_Reports = PythonOperator(
        task_id = 'WF_Uptown_DBF_to_Sales_Reports',
        python_callable = _DBF_to_Sales_Reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_SDET': 'name_of_key',
                   'Key_SLS': 'name_of_key',
                   'Key_PAGES': 'name_of_key',
                   'Key_MENU': 'name_of_key',
                   'Key_PAGETYPE': 'name_of_key',
                   'Key_REVCENT': 'name_of_key',
                   'Key_TIPOPAG': 'name_of_key',
                   'Target_Key_SDET': 'name_of_key',
                   'Target_Key_SLS': 'name_of_key',
                   'Target_Key_PAGES': 'name_of_key',
                   'Target_Key_MENU': 'name_of_key',
                   'Target_Key_PAGETYPE': 'name_of_key',
                   'Target_Key_REVCENT': 'name_of_key',
                   'Target_Key_TIPOPAG': 'name_of_key',
                   'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
                   'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
                   'Target_Key_GROUP_SALES': 'name_of_key',
                   'Target_Key_RC_SALES': 'name_of_key',
                   'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
                   'Target_Key_ITEM_SALES_RC': 'name_of_key',
                   'Target_Key_SUMIF_RC': 'name_of_key',
                   'Target_Key_SUMIF_GRP': 'name_of_key',
                   'Branch': 'WF Uptown'}
    )

    WFI_BGC_DBF_to_Sales_Reports = PythonOperator(
        task_id = 'WFI_BGC_DBF_to_Sales_Reports',
        python_callable = _DBF_to_Sales_Reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_SDET': 'name_of_key',
                   'Key_SLS': 'name_of_key',
                   'Key_PAGES': 'name_of_key',
                   'Key_MENU': 'name_of_key',
                   'Key_PAGETYPE': 'name_of_key',
                   'Key_REVCENT': 'name_of_key',
                   'Key_TIPOPAG': 'name_of_key',
                   'Target_Key_SDET': 'name_of_key',
                   'Target_Key_SLS': 'name_of_key',
                   'Target_Key_PAGES': 'name_of_key',
                   'Target_Key_MENU': 'name_of_key',
                   'Target_Key_PAGETYPE': 'name_of_key',
                   'Target_Key_REVCENT': 'name_of_key',
                   'Target_Key_TIPOPAG': 'name_of_key',
                   'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
                   'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
                   'Target_Key_GROUP_SALES': 'name_of_key',
                   'Target_Key_RC_SALES': 'name_of_key',
                   'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
                   'Target_Key_ITEM_SALES_RC': 'name_of_key',
                   'Target_Key_SUMIF_RC': 'name_of_key',
                   'Target_Key_SUMIF_GRP': 'name_of_key',
                   'Branch': 'WFI BGC'}
    )

    # Generates necessary reports
    # LF_BGC_generate_reports = PythonOperator(
    #     task_id = 'LF_BGC_generate_reports',
    #     python_callable = _generate_reports,
    #     op_kwargs={'Bucket': 'name_of_bucket',
    #                'Key_SDET': 'name_of_key',
    #                'Key_SLS': 'name_of_key',
    #                'Key_PAGES': 'name_of_key',
    #                'Key_MENU': 'name_of_key',
    #                'Key_PAGETYPE': 'name_of_key',
    #                'Key_REVCENT': 'name_of_key',
    #                'Key_TIPOPAG': 'name_of_key',
    #                'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
    #                'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
    #                'Target_Key_GROUP_SALES': 'name_of_key',
    #                'Target_Key_RC_SALES': 'name_of_key',
    #                'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
    #                'Target_Key_ITEM_SALES_RC': 'name_of_key',
    #                'Target_Key_SUMIF_RC': 'name_of_key',
    #                'Target_Key_SUMIF_GRP': 'name_of_key',
    #                'Branch': 'LF BGC'
    #                }
    # )

    # WF_BGC_generate_reports = PythonOperator(
    #     task_id = 'WF_BGC_generate_reports',
    #     python_callable = _generate_reports,
    #     op_kwargs={'Bucket': 'name_of_bucket',
    #                'Key_SDET': 'name_of_key',
    #                'Key_SLS': 'name_of_key',
    #                'Key_PAGES': 'name_of_key',
    #                'Key_MENU': 'name_of_key',
    #                'Key_PAGETYPE': 'name_of_key',
    #                'Key_REVCENT': 'name_of_key',
    #                'Key_TIPOPAG': 'name_of_key',
    #                'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
    #                'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
    #                'Target_Key_GROUP_SALES': 'name_of_key',
    #                'Target_Key_RC_SALES': 'name_of_key',
    #                'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
    #                'Target_Key_ITEM_SALES_RC': 'name_of_key',
    #                'Target_Key_SUMIF_RC': 'name_of_key',
    #                'Target_Key_SUMIF_GRP': 'name_of_key',
    #                'Branch': 'WF BGC'
    #                }
    # )

    # WF_Greenhills_generate_reports = PythonOperator(
    #     task_id = 'WF_Greenhills_generate_reports',
    #     python_callable = _generate_reports,
    #     op_kwargs={'Bucket': 'name_of_bucket',
    #                'Key_SDET': 'name_of_key',
    #                'Key_SLS': 'name_of_key',
    #                'Key_PAGES': 'name_of_key',
    #                'Key_MENU': 'name_of_key',
    #                'Key_PAGETYPE': 'name_of_key',
    #                'Key_REVCENT': 'name_of_key',
    #                'Key_TIPOPAG': 'name_of_key',
    #                'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
    #                'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
    #                'Target_Key_GROUP_SALES': 'name_of_key',
    #                'Target_Key_RC_SALES': 'name_of_key',
    #                'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
    #                'Target_Key_ITEM_SALES_RC': 'name_of_key',
    #                'Target_Key_SUMIF_RC': 'name_of_key',
    #                'Target_Key_SUMIF_GRP': 'name_of_key',
    #                'Branch': 'WF Greenhills'
    #                }
    # )

    # WF_Podium_generate_reports = PythonOperator(
    #     task_id = 'WF_Podium_generate_reports',
    #     python_callable = _generate_reports,
    #     op_kwargs={'Bucket': 'name_of_bucket',
    #                'Key_SDET': 'name_of_key',
    #                'Key_SLS': 'name_of_key',
    #                'Key_PAGES': 'name_of_key',
    #                'Key_MENU': 'name_of_key',
    #                'Key_PAGETYPE': 'name_of_key',
    #                'Key_REVCENT': 'name_of_key',
    #                'Key_TIPOPAG': 'name_of_key',
    #                'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
    #                'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
    #                'Target_Key_GROUP_SALES': 'name_of_key',
    #                'Target_Key_RC_SALES': 'name_of_key',
    #                'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
    #                'Target_Key_ITEM_SALES_RC': 'name_of_key',
    #                'Target_Key_SUMIF_RC': 'name_of_key',
    #                'Target_Key_SUMIF_GRP': 'name_of_key',
    #                'Branch': 'WF Podium'
    #                }
    # )

    # WF_Rada_generate_reports = PythonOperator(
    #     task_id = 'WF_Rada_generate_reports',
    #     python_callable = _generate_reports,
    #     op_kwargs={'Bucket': 'name_of_bucket',
    #                'Key_SDET': 'name_of_key',
    #                'Key_SLS': 'name_of_key',
    #                'Key_PAGES': 'name_of_key',
    #                'Key_MENU': 'name_of_key',
    #                'Key_PAGETYPE': 'name_of_key',
    #                'Key_REVCENT': 'name_of_key',
    #                'Key_TIPOPAG': 'name_of_key',
    #                'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
    #                'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
    #                'Target_Key_GROUP_SALES': 'name_of_key',
    #                'Target_Key_RC_SALES': 'name_of_key',
    #                'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
    #                'Target_Key_ITEM_SALES_RC': 'name_of_key',
    #                'Target_Key_SUMIF_RC': 'name_of_key',
    #                'Target_Key_SUMIF_GRP': 'name_of_key',
    #                'Branch': 'WF Rada'
    #                }
    # )

    # WF_Rockwell_generate_reports = PythonOperator(
    #     task_id = 'WF_Rockwell_generate_reports',
    #     python_callable = _generate_reports,
    #     op_kwargs={'Bucket': 'name_of_bucket',
    #                'Key_SDET': 'name_of_key',
    #                'Key_SLS': 'name_of_key',
    #                'Key_PAGES': 'name_of_key',
    #                'Key_MENU': 'name_of_key',
    #                'Key_PAGETYPE': 'name_of_key',
    #                'Key_REVCENT': 'name_of_key',
    #                'Key_TIPOPAG': 'name_of_key',
    #                'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
    #                'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
    #                'Target_Key_GROUP_SALES': 'name_of_key',
    #                'Target_Key_RC_SALES': 'name_of_key',
    #                'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
    #                'Target_Key_ITEM_SALES_RC': 'name_of_key',
    #                'Target_Key_SUMIF_RC': 'name_of_key',
    #                'Target_Key_SUMIF_GRP': 'name_of_key',
    #                'Branch': 'WF Rockwell'
    #                }
    # )

    # WF_Salcedo_generate_reports = PythonOperator(
    #     task_id = 'WF_Salcedo_generate_reports',
    #     python_callable = _generate_reports,
    #     op_kwargs={'Bucket': 'name_of_bucket',
    #                'Key_SDET': 'name_of_key',
    #                'Key_SLS': 'name_of_key',
    #                'Key_PAGES': 'name_of_key',
    #                'Key_MENU': 'name_of_key',
    #                'Key_PAGETYPE': 'name_of_key',
    #                'Key_REVCENT': 'name_of_key',
    #                'Key_TIPOPAG': 'name_of_key',
    #                'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
    #                'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
    #                'Target_Key_GROUP_SALES': 'name_of_key',
    #                'Target_Key_RC_SALES': 'name_of_key',
    #                'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
    #                'Target_Key_ITEM_SALES_RC': 'name_of_key',
    #                'Target_Key_SUMIF_RC': 'name_of_key',
    #                'Target_Key_SUMIF_GRP': 'name_of_key',
    #                'Branch': 'WF Salcedo'
    #                }
    # )

    # WF_Uptown_generate_reports = PythonOperator(
    #     task_id = 'WF_Uptown_generate_reports',
    #     python_callable = _generate_reports,
    #     op_kwargs={'Bucket': 'name_of_bucket',
    #                'Key_SDET': 'name_of_key',
    #                'Key_SLS': 'name_of_key',
    #                'Key_PAGES': 'name_of_key',
    #                'Key_MENU': 'name_of_key',
    #                'Key_PAGETYPE': 'name_of_key',
    #                'Key_REVCENT': 'name_of_key',
    #                'Key_TIPOPAG': 'name_of_key',
    #                'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
    #                'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
    #                'Target_Key_GROUP_SALES': 'name_of_key',
    #                'Target_Key_RC_SALES': 'name_of_key',
    #                'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
    #                'Target_Key_ITEM_SALES_RC': 'name_of_key',
    #                'Target_Key_SUMIF_RC': 'name_of_key',
    #                'Target_Key_SUMIF_GRP': 'name_of_key',
    #                'Branch': 'WF Uptown'
    #                }
    # )

    # WFI_BGC_generate_reports = PythonOperator(
    #     task_id = 'WFI_BGC_generate_reports',
    #     python_callable = _generate_reports,
    #     op_kwargs={'Bucket': 'name_of_bucket',
    #                'Key_SDET': 'name_of_key',
    #                'Key_SLS': 'name_of_key',
    #                'Key_PAGES': 'name_of_key',
    #                'Key_MENU': 'name_of_key',
    #                'Key_PAGETYPE': 'name_of_key',
    #                'Key_REVCENT': 'name_of_key',
    #                'Key_TIPOPAG': 'name_of_key',
    #                'Key_GROUP_LOOKUP': 'name_of_group_lookup_file',
    #                'Key_RC_LOOKUP': 'name_of_rc_lookup_file',
    #                'Target_Key_GROUP_SALES': 'name_of_key',
    #                'Target_Key_RC_SALES': 'name_of_key',
    #                'Target_Key_ITEM_SALES_GROUP': 'name_of_key',
    #                'Target_Key_ITEM_SALES_RC': 'name_of_key',
    #                'Target_Key_SUMIF_RC': 'name_of_key',
    #                'Target_Key_SUMIF_GRP': 'name_of_key',
    #                'Branch': 'WFI BGC'
    #                }
    # )

    merge_reports = BranchPythonOperator(
        task_id = 'merge_reports',
        python_callable = _merge_reports,
        op_kwargs={'Bucket': 'name_of_bucket',
                   'Key_LF_BGC_ISG': 'name_of_key',
                   'Key_WF_BGC_ISG': 'name_of_key',
                   'Key_WF_GREENHILLS_ISG': 'name_of_key',
                   'Key_WF_PODIUM_ISG': 'name_of_key',
                   'Key_WF_RADA_ISG': 'name_of_key',
                   'Key_WF_ROCKWELL_ISG': 'name_of_key',
                   'Key_WF_SALCEDO_ISG': 'name_of_key',
                   'Key_WF_UPTOWN_ISG': 'name_of_key',
                   'Key_WFI_BGC_ISG': 'name_of_key',
                   'Target_Key_ISG': 'name_of_key',
                   }
    )

    success = BashOperator(
        task_id = 'success',
        bash_command = "echo 'Pipeline success'"
    )

    failure = BashOperator(
        task_id = 'failure',
        bash_command = "echo 'Pipeline failed'"
    )

    [LF_BGC_DBF_to_Sales_Reports,
     WF_BGC_DBF_to_Sales_Reports,
     WF_Greenhills_DBF_to_Sales_Reports, 
     WF_Podium_DBF_to_Sales_Reports, 
     WF_Rada_DBF_to_Sales_Reports, 
     WF_Rockwell_DBF_to_Sales_Reports, 
     WF_Salcedo_DBF_to_Sales_Reports, 
     WF_Uptown_DBF_to_Sales_Reports, 
     WFI_BGC_DBF_to_Sales_Reports] >> merge_reports >> [success, failure]