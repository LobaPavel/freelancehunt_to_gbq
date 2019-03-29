import hmac
import hashlib
import base64
from google.cloud import bigquery
import json, requests, io, os




def sign(api_secret, url, method, post_params=""):
    message =url+ method+post_params
    key = api_secret.encode('utf-8')
    message = message.encode('utf-8')
    return base64.b64encode(hmac.new(key, msg=message, digestmod=hashlib.sha256).digest())



def get_jobs(id, secret, skills):

    method = 'GET'
    response_length = 1
    page_number = 0
    jobs = []

    while response_length > 0:

        page_number += 1
        url = 'https://api.freelancehunt.com/projects?per_page=50&page='+str(page_number)+'&skills=' + skills
        response = requests.get(url, auth=(id, sign(secret, url, method))).json()
        response_length = len(response)
        jobs += response
        print('Current page number is {}'.format(str(page_number)))

    print('Total scanned jobs is {}'.format(str(len(jobs))))

    return jobs



def get_job_details(id, secret, project_id):

    method = 'GET'

    url = 'https://api.freelancehunt.com/projects/' + project_id
    response = requests.get(url, auth=(id, sign(secret, url, method))).json()

    return response



def get_job_bids(id, secret, project_id):

    method = 'GET'

    url = 'https://api.freelancehunt.com/projects/' + project_id + '/bids'
    response = requests.get(url, auth=(id, sign(secret, url, method))).json()

    return response



def get_freelancer_portfolio(id, secret, login):

    method = 'GET'

    url = 'https://api.freelancehunt.com/profiles/'+ login +'?include=portfolio'
    response = requests.get(url, auth=(id, sign(secret, url, method))).json()

    return response



def load_to_gbq(json_file, project_id, dataset_id, entity, w_disposition = "WRITE_APPEND", update_option = ["ALLOW_FIELD_ADDITION"]):
    """
        Loading data to BigQuery using *bq_configuration* settings.
    """
    # construct Client object with the path to the table in which data will be stored
    client = bigquery.Client(project = project_id)
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(entity)

    # determine uploading options
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = w_disposition
    job_config.max_bad_records = 1
    job_config.source_format = "NEWLINE_DELIMITED_JSON"
    job_config.autodetect = True
    job_config.schema_update_options = update_option


    # upload the file to BigQuery table
    job = client.load_table_from_file(json_file, table_ref, job_config = job_config)
    job.result()
    print('The Job ' + job.job_id + ' in status ' + job.state + ' for table ' + project_id + '.' + dataset_id + '.' + entity + '.')




def main(request):
#def main():
    """
        Function to execute.
    """
    try:
        # get POST data from Flask.request object
        #request_json = request.get_json()

        # add your freelancehunt and gbq account details
        request_json = {
                'freelancehunt_id': 'freelancehunt_id',
                'freelancehunt_secret':'freelancehunt_secret',
                'freelancehunt_skills':'1,2,6,13,14,22,23,24,28,38,41,43,45,48,54,56,57,62,64,68,76,78,86,89,94,96,97,99,103,104,109,111,112,120,121,124,125,127,129,131,133,134,135,136,138,144,145,146,150,151,154,160,162,169,170,173,174,175,177,178,179,180,182',
                'bq_project_id': 'bq_project_id',
                'bq_dataset_id': 'bq_dataset_id'
            }

        freelancehunt_id = request_json['freelancehunt_id']
        freelancehunt_secret = request_json['freelancehunt_secret']
        freelancehunt_skills = request_json['freelancehunt_skills']
        bq_project_id = request_json['bq_project_id']
        bq_dataset_id = request_json['bq_dataset_id']

    except Exception as error:
        print("An error occured during proccessing of POST-request body. Error details: {}".format(str(error)))
        raise SystemExit


    # getting list of job offers and load it to GBQ
    # Build the service object.
    client = bigquery.Client(project = bq_project_id)

    query = 'SELECT project_id FROM `'+bq_project_id+'.freelance_hunt.jobs_list`'

    # running query
    query_job = client.query(query)
    rows = query_job.result()

    collected_jobs = []
    collected_jobs_qty = 0

    for row in rows:
        collected_jobs_qty += 1
        collected_jobs.append(str(row.project_id))

    print('The number of already collected jobs is {}.'.format(str(collected_jobs_qty)))

    jobs = get_jobs(freelancehunt_id, freelancehunt_secret, freelancehunt_skills)

    json_jobs = ''
    new_jobs = 0

    for row in jobs:
        if row.get('project_id') not in collected_jobs:
            new_jobs += 1
            json_jobs += json.dumps(row) + '\n'

    print('The number of new collected jobs is {}.'.format(str(new_jobs)))

    if len(json_jobs) > 0:
        json_jobs = json_jobs.encode("utf-8")
        json_jobs = io.BytesIO(json_jobs)

        load_to_gbq(json_jobs, bq_project_id, bq_dataset_id, 'jobs_list')


    # Starting to collect job details
    query = """SELECT
                  jobs_list.project_id project_id
                FROM
                  dataset_id.jobs_list jobs_list
                LEFT JOIN (
                  SELECT
                    project_id
                  FROM
                    dataset_id.job_details) job_details
                ON
                  jobs_list.project_id = job_details.project_id
                WHERE
                  job_details.project_id IS NULL
                  LIMIT 250"""

    # running query
    query_job = client.query(query)
    rows = query_job.result()

    json_job_details = ''
    collected_job_details = 0

    if rows:
        for row in rows:
            collected_job_details += 1
            job_details = get_job_details(freelancehunt_id, freelancehunt_secret, str(row.project_id))
            if job_details.get('skills'):
                job_details.pop('skills')
            if job_details.get('payment_types'):
                job_details.pop('payment_types')
            if job_details.get('features'):
                job_details.pop('features')
            json_job_details += json.dumps(job_details) + '\n'

        print('The number of new collected job details is {}.'.format(str(collected_job_details)))
        json_job_details = json_job_details.encode("utf-8")
        json_job_details = io.BytesIO(json_job_details)

        load_to_gbq(json_job_details, bq_project_id, bq_dataset_id, 'job_details')



    # Starting to collect bids
    query = """SELECT
                  jobs_list.project_id project_id
                FROM
                  dataset_id.jobs_list jobs_list
                LEFT JOIN (
                  SELECT
                    project_id
                  FROM
                    dataset_id.job_bids) job_bids
                ON
                  jobs_list.project_id = job_bids.project_id
                WHERE
                  job_bids.project_id IS NULL
                LIMIT
                  250
"""

    # running query
    query_job = client.query(query)
    rows = query_job.result()

    json_job_bids = ''
    collected_job_bids = 0

    if rows:
        for row in rows:
            collected_job_bids += 1
            job_bids = get_job_bids(freelancehunt_id, freelancehunt_secret, str(row.project_id))
            for bid in job_bids:
                bid['project_id'] = row.project_id
                json_job_bids += json.dumps(bid) + '\n'

        print('The number of new collected job bids is {}.'.format(str(collected_job_bids)))
        json_job_bids = json_job_bids.encode("utf-8")
        json_job_bids = io.BytesIO(json_job_bids)

        load_to_gbq(json_job_bids, bq_project_id, bq_dataset_id, 'job_bids')
