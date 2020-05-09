import logging, os
import azure.functions as func

from github import Github
import mysql.connector
from urllib.parse import urlparse

def main(req: func.HttpRequest) -> func.HttpResponse:

    req_body = req.get_json()

    #isDismissed = req_body.get('action') == 'dismissed'
    #isOpen = req_body.get('pull_request').get('state') == 'open'
    #isDraft = req_body.get('pull_request').get('draft')

    reviewId = req_body.get('review').get('id')
    submittedAt = req_body.get('review').get('submitted_at')
    commitId = req_body.get('review').get('commit_id')
    reviewerLogin = req_body.get('review').get('user').get('login')
    reviewState = req_body.get('review').get('state')
    prNumber = req_body.get('pull_request').get('number')

    if reviewState == 'commented':
        logging.info("Skipping, comment. reviewer={},pr_id={}".format(reviewerLogin, prNumber))
        return func.HttpResponse('Comment ignored.')

    if reviewerLogin not in data_engineering_members():
        logging.info("Skipping, non-DE reviewer. reviewer={},pr_id={}".format(reviewerLogin, prNumber))
        return func.HttpResponse('non-DE reviewer ignored')

    sql_client = get_sql_client()
    insert_review_event(sql_client, reviewId, submittedAt, commitId, reviewerLogin, reviewState, prNumber)

    return func.HttpResponse('Processed event successfully.')

def add_tag_to_pull_request(ghClient, repoName, prNumber, tag):
    repo = ghClient.get_repo(repoName)
    pr = repo.get_pull(prNumber)

    pr.add_to_labels(tag)

def data_engineering_members():
    return [
        'albertomsp',
        'bxw11',
        'davigust',
        'ikewalker',
        'kimyen',
        'kosinsky',
        'msempere',
        'preston-m-price',
        'trevorc-gh',
        'jeffsvajlenko',
        'jamisonhyatt',
        'tbries',
        'sdseaton',
        'mbellani',
        'whoahbot'
    ]

def insert_review_event(sql_client, review_id, submitted_at, commit_id, reviewer_login, review_state, pull_request_number):
    sql_client.cursor().execute("""
        INSERT INTO review_history (
            review_id,
            submitted_at,
            commit_id,
            reviewer_login,
            review_state,
            pull_request_number) 
        VALUES (%s,%s,%s,%s,%s,%s)""",
        (review_id, submitted_at, commit_id, reviewer_login, review_state, pull_request_number))
    sql_client.commit()

def get_sql_client():
    conn = os.environ["db_connection_string"]
    conn_dict = dict(item.split("=") for item in conn.split(","))

    return mysql.connector.connect(
        user=conn_dict["user"],
        password=conn_dict["password"],
        host=conn_dict["host"],
        port=conn_dict["port"],
        database=conn_dict["database"]
    )