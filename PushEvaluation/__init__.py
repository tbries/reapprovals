import logging, os
import azure.functions as func

from github import Github
import mysql.connector
from urllib.parse import urlparse

def main(req: func.HttpRequest) -> func.HttpResponse:

    req_body = req.get_json()

    #isOpen = req_body.get('pull_request').get('state') == 'open'
    #isDraft = req_body.get('pull_request').get('draft')

    reviewId = req_body.get('review').get('id')
    submittedAt = req_body.get('review').get('submitted_at')
    commitId = req_body.get('review').get('commit_id')
    reviewerLogin = req_body.get('review').get('user').get('login')
    reviewState = req_body.get('review').get('state')
    prNumber = req_body.get('pull_request').get('number')

    if reviewState == 'commented':
        logging.info("TRACE: Skipping, comment. reviewer={}, pr={}".format(reviewerLogin, prNumber))
        return func.HttpResponse('Comment ignored.')

    if reviewerLogin not in data_engineering_members():
        logging.info("TRACE: Skipping, non-DE reviewer. reviewer={}, pr={}".format(reviewerLogin, prNumber))
        return func.HttpResponse('non-DE reviewer ignored')

    sql_client = get_sql_client()

    if is_dismissed_approval(sql_client, reviewId, reviewState):
        logging.info("TRACE: Dismissed approval: review_id={}, reviewer={}, pr={}".format(reviewId, reviewerLogin, prNumber))
        add_tag_to_pull_request('github/airflow-sources', prNumber, 'DE Approval Dismissed')

    insert_review_event(sql_client, reviewId, submittedAt, commitId, reviewerLogin, reviewState, prNumber)

    if reviewState == 'approved':
        logging.info("TRACE: DE approval: review_id={}, reviewer={}, pr={}".format(reviewId, reviewerLogin, prNumber))
        remove_tag_from_pull_request('github/airflow-sources', prNumber, 'DE Approval Dismissed')

    return func.HttpResponse('Processed event successfully.')


def add_tag_to_pull_request(repoName, pr_number, tag):

    pat = os.environ["github_pat"]
    github_client = Github(pat)

    repo = github_client.get_repo(repoName)
    pr = repo.get_pull(pr_number)

    pr.add_to_labels(tag)


def remove_tag_from_pull_request(repoName, pr_number, tag):

    pat = os.environ["github_pat"]
    github_client = Github(pat)

    repo = github_client.get_repo(repoName)
    pr = repo.get_pull(pr_number)

    if tag in [label.name for label in pr.labels]:
        pr.remove_from_labels(tag)


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


def is_dismissed_approval(sql_client, review_id, new_state):

    if new_state != 'dismissed':
        return False

    cursor = sql_client.cursor()

    cursor.execute("""
        SELECT
            review_state 
        FROM
            reapprovals.review_history
        WHERE
            review_id = %s""",
        (review_id,))

    records = cursor.fetchall()

    for row in records:
        if row[0] == 'approved':
            return True

    return False


def get_sql_client():
    conn = os.environ["db_connection_string"]
    conn_dict = dict(item.split("=") for item in conn.split(","))

    return mysql.connector.connect(
        user=conn_dict["user"],
        password=conn_dict["password"],
        host=conn_dict["host"],
        port=conn_dict["port"],
        database=conn_dict["database"])