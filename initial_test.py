print('Script started.')
import requests, random, time, pandas as pd, civis, datetime, os, sys
import lxml.html as LH

def text(elt):
    return elt.text_content().replace(u'\xa0', u' ')

def dict_merge(parent_dic, added_dic):
    for obj in list(added_dic.keys()):
        parent_dic[obj]=added_dic.get(obj)
    return parent_dic

def changetoint(dataframe, columnlist):
    dataframe[columnlist] = dataframe[columnlist].fillna(0.0).astype(int)

def available_races(tree):
    election_types = ['General', 'Primary', 'Special_General', 'Special_Primary', 'Special']
    these_elections = []
    for etype in election_types:
        try:
            tree.get_element_by_id(etype)
            these_elections.append(etype)
        except:
            pass
    return these_elections

def available_offices(tree):
    offices = ['US-Senate', 'US-House', 'Statewide', 'State-Senate', 'State-House']
    these_offices = []
    for office in offices:
        try:
            tree.get_element_by_id(office)
            these_offices.append(office)
        except:
            pass
    return these_offices

def get_candidate(candidate_class_element):
    candidate_data = {}
    candidate = candidate_class_element
    name = ''
    endorsed = ''
    cand_party = ''
    current_status = ''
    grade = ''
    party = ''

    try:
        name = candidate.find_class('candidate-name')[0].text_content().strip()
    except:
        name = ''
    if name[0]=='*':
        name = name[1:]
    try:
        grade = candidate.find_class('candidate-grade')[0].text_content().strip()
    except:
        grade = ''

    try:
        if candidate.get('class') == 'print-candidate candidate-endorsed-true':
            endorsed = 'Y'
        else:
            endorsed = 'N'
    except:
        endorsed = ''
    try:
        if len(candidate.find_class('candidate-endorsed')[0].getchildren()) > 0:
            endorsed = 'Y'
        else:
            endorsed = 'N'
    except:
        pass
    try:
        cand_party = candidate.find_class('candidate-incumbent')[0].text_content().strip()
    except:
        cand_party = ''

    try:
        current_status = cand_party[:cand_party.find('(')-1].strip()
    except:
        current_status = ''
    try:
        party = cand_party[-2]
    except:
        party = ''

    candidate_data['name']=name
    candidate_data['cand_party'] = cand_party
    candidate_data['party'] = party
    candidate_data['current_status'] = current_status
    candidate_data['endorsed']=endorsed
    candidate_data['grade']=grade
    return candidate_data

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

def nameparse(list_of_tuples):

    parsed_name = {'PrefixMarital' : '', 'PrefixOther' : '', 'GivenName' : '',
                   'FirstInitial' : '', 'MiddleName' : '', 'MiddleInitial' : '',
                   'Surname' : '', 'LastInitial' :  '', 'SuffixGenerational' : '',
                   'SuffixOther' : '', 'Nickname' : '', 'Other' : ''}
    for pair in list_of_tuples:
        existing = parsed_name.get(pair[1])
        if existing == None:
            parsed_name['Other']= str(parsed_name.get('Other') + ' '
                                      + str(pair[0])).strip()
        else:
            parsed_name[pair[1]]= str(existing + ' ' + str(pair[0])).strip()
        first_name = ''
        middle_name = ''
        last_name = ''
        if parsed_name.get('GivenName')=='':
            first_name = parsed_name.get('FirstInitial')
        else:
            first_name = parsed_name.get('GivenName')

        if parsed_name.get('MiddleName')=='':
            middle_name = parsed_name.get('MiddleInitial')
        else:
            middle_name = parsed_name.get('MiddleName')

        if parsed_name.get('Surname')=='':
            last_name = parsed_name.get('LastInitial')
        else:
            last_name = parsed_name.get('Surname')

        if first_name == '' and last_name == '' and parsed_name.get('Other') != '':
            first_name = parsed_name['Other'][:parsed_name['Other'].find(' ')].strip()
            last_name = parsed_name['Other'][parsed_name['Other'].rfind(' '):].strip()

    final_name = {'prefix' : (parsed_name.get('PrefixMarital') + ' ' + parsed_name.get('PrefixOther')).strip(),
                  'first_name' : first_name, 'middle_name' : middle_name, 'last_name' :last_name,
                  'suffix' : (parsed_name.get('SuffixGenerational') + ' ' + parsed_name.get('SuffixOther')).strip(),
                  'nickname' : parsed_name.get('Nickname'), 'other' : parsed_name.get('Other')}

    return(final_name)

def cdf(col_list):
    df = pd.DataFrame(columns=col_list, index = ())
    return df

nra_grade_columns = ['state', 'year', 'race', 'date', 'level', 'contest', 'district', 'name', 'cand_party',
                     'party', 'current_status', 'endorsed', 'grade']
nra_grades = cdf(nra_grade_columns)

print('Created NRA df.')

run_log_columns = ['runtime', 'records', 'next_run']
run_log = cdf(run_log_columns)
print('Created run log columns.')

state_time_col = ['runtime', 'state']

client = civis.APIClient()
db_id = client.get_database_id('Everytown for Gun Safety')

abb_sname = {}
sname_abb = {}

def now():
    '''
    Returns a timestamp as a string.
    '''
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return timestamp

def pad(num):
    strnum = str(num)
    if len(strnum) == 1:
        strnum = '0' + strnum
    return strnum

## First, determine next runtime

runtime = now()
nextday = random.randrange(1,7)
nexthour = random.randrange(1,24)
nextmin = random.randrange(1,60)

min = 0
if nextmin + int(now()[14:16])>=60:
    nexthour+=1
    min = nextmin + int(now()[14:16]) - 60
else:
    min = nextmin + int(now()[14:16])

min = pad(min)

hour = 0
if nexthour + int(now()[11:13])>=24:
    nextday+=1
    hour = nexthour + int(now()[11:13]) - 23
else:
    hour = nexthour + int(now()[11:13])
hour = pad(hour)

time_of_run = hour +':' + min + ':00'

day = 0
month = int(now()[5:7])
ryear = int(now()[:4])

if nextday + int(now()[8:10])>=28:
    month+=1
    day = nextday + int(now()[8:10]) - 27
else:
    day = nextday + int(now()[8:10])

if month > 12:
    month = 1
    ryear+=1

month = pad(month)
day = pad(day)

date_of_run = (str(ryear) + '-' + month + '-' + day)
nextrun = date_of_run + ' ' + time_of_run

print('Determined next run time: {}.'.format(nextrun))

print('Getting states.')
states = civis.io.read_civis(table='legiscan.ls_state',
                             database='Everytown for Gun Safety', use_pandas=True)
state_record_dic = states.to_dict('records')
print('Retrieving state records.')

state_ids = {}
for record in state_record_dic:
    state_ids[record.get('state_name')]=record.get('state_abbr')

for state in list(state_ids.keys()):
    print('Now working on {}.'.format(state))
    state_time = cdf(state_time_col)
    state_time = state_time.append({'runtime' : now(), 'state' : state}, ignore_index=True)
    import_update = civis.io.dataframe_to_civis(df=state_time, database = db_id,
                                                 table='amydrummond.state_update_log', max_errors = 2,
                                                 existing_table_rows = 'append', polling_interval=10)
    civis_upload_status(import_update, 'state log')
    url = 'https://www.nrapvf.org/grades/archive/' + state.replace(' ', '%20')
    try:
        r = requests.get(url)
        root = LH.fromstring(r.content)
        html_doc = LH.document_fromstring(r.content)

        races = available_races(html_doc)
        for race in races:
            row = {}
            row['state']=state_ids.get(state)
            row['year']=now()[:4]
            race_section = html_doc.get_element_by_id(race)
            election_group = race_section.find_class('election-group')[0]
            election_date = election_group.find_class('election-date')[0].text_content().strip()
            offices_up = available_offices(election_group)
            row['race']=race
            row['date']=election_date
            for level in offices_up:
                row['level']=level
                lev = election_group.get_element_by_id(level)
                specific_elections = lev.getchildren()
                for contest in specific_elections:
                    row['contest']=contest[0].text_content().strip()
                    district = contest.find_class('election-location')[0].text_content().strip()
                    row['district']=district
                    candidates = contest.find_class('print-candidate')
                    for candidate in candidates:
                        row = dict_merge(row,get_candidate(candidate))
                        nra_grades = nra_grades.append({'state' : row.get('state'), 'year' : row.get('year'),
                        'race' : row.get('race'), 'date' : row.get('date'), 'level' : row.get('level'),
                        'contest' : row.get('contest'), 'district' : row.get('district'), 'name' : row.get('name'),
                        'cand_party' : row.get('cand_party'), 'party' : row.get('party'), 'current_status' : row.get('current_status'),
                        'endorsed' : row.get('endorsed'), 'grade' : row.get('grade')}, ignore_index=True)
        print('Data loaded. Naptime...')
        time.sleep(random.randrange(0,100))
    except:
        print("Unable to get records for {} from {}.".format(state, now()[:4]))
        time.sleep(random.randrange(0,60))

    changetoint(nra_grades, ['year'])
    run_log = run_log.append({'runtime' : runtime, 'records' : len(nra_grades), 'next_run' : nextrun}, ignore_index=True)
    print("Uploading to Civis.")
    print('Now uploading session changes. There are {} records.'.format(len(nra_grades)))
    import_summary = civis.io.dataframe_to_civis(df=nra_grades, database = db_id,
                                                 table='amydrummond.nra_grades_t', max_errors = 2,
                                                 existing_table_rows = 'truncate', polling_interval=10)
    civis_upload_status(import_summary, 'nra grades')

    import_log = civis.io.dataframe_to_civis(df=run_log, database = db_id,
                                                 table='state_leg.nra_log', max_errors = 2,
                                                 existing_table_rows = 'append', polling_interval=10)
    civis_upload_status(import_summary, 'nra log')

    print('All records have been uploaded.')


