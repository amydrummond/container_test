import requests, random, time, pandas, civis, datetime, os, sys

client = civis.APIClient()
db_id = client.get_database_id('Everytown for Gun Safety')

def cdf(col_list):
    df = pandas.DataFrame(columns=col_list, index=())
    return df


def now():
    '''
    Returns a timestamp as a string.
    '''
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return timestamp

runtime = now()


def civis_upload_status(civis_upload, data_name='data'):
    while civis_upload._civis_state in ['queued', 'running']:
        print("waiting...")
        time.sleep(10)
    print(civis_upload._civis_state)

    if civis_upload._civis_state == 'failed':
        print("Import to Civis failed.")
        print(civis_upload.result(10))
        print("Ending without completing ...")
        sys.exit(1)
    else:
        print("New {} has been uploaded to Civis Platform.".format(data_name))

s_cols = ['name', 'abbreviation', 'timestamp']
test_upload = cdf(s_cols)

states = civis.io.read_civis(table='legiscan.ls_state',
                             database='Everytown for Gun Safety', use_pandas=True)
state_record_dic = states.to_dict('records')


for record in state_record_dic:
    n = record.get('state_name')
    a = record.get('state_abbr')
    t = now()
    test_upload = test_upload.append({'name' : n, 'abbreviation' : a, 'timestamp' : t},
                                     ignore_index=True)

    print("Uploading to Civis.")
print('Now uploading session changes. There are {} records.'.format(len(test_upload)))
import_summary = civis.io.dataframe_to_civis(df=test_upload, database = db_id, table='amydrummond.test_upload',
                                             max_errors = 2,existing_table_rows = 'drop', polling_interval=10)

civis_upload_status(import_summary, 'test_upload')
print('Done.')
