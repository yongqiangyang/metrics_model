import math
import pendulum
import pandas as pd

from perceval.backend import uuid
from datetime import datetime, timedelta
from urllib.parse import urlparse
import json
import yaml
import logging
from grimoire_elk.enriched.utils import get_time_diff_days
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                            datetime_to_utc,
                                          str_to_datetime)
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
from grimoire_elk.elastic import ElasticSearch


BACKOFF_FACTOR = 0.2
MAX_RETRIES = 21
MAX_RETRIES_ON_REDIRECT = 5
MAX_RETRIES_ON_READ = 8
MAX_RETRIES_ON_CONNECT = 21
STATUS_FORCE_LIST = [408, 409, 429, 502, 503, 504]
METADATA_FILTER_RAW = 'metadata__filter_raw'
REPO_LABELS = 'repository_labels'

######################### 代码开发活动 #############################################
LOC_FREQUENCY_WEIGHT = 0.0729
CONTRIBUTOR_COUNT_WEIGHT = 0.2517
PR_COUNT_WEIGHT = 0.1582
COMMIT_FREQUENCY_WEIGHT = 0.2033
UPDATED_ISSUES_WEIGHT = 0.1426
IS_MAINTAINED_WEIGHT = 0.1714

LOC_FREQUENCY_THRESHOLD = 300000
CONTRIBUTOR_COUNT_THRESHOLD = 1000
PR_COUNT_THRESHOLD = 10000
COMMIT_FREQUENCY_THRESHOLD = 1000
UPDATED_ISSUES_THRESHOLD = 2500
IS_MAINTAINED_THRESHOLD = 1

LOC_FREQUENCY_MULTIPLE_THRESHOLD = 300000
CONTRIBUTOR_COUNT_MULTIPLE_THRESHOLD = 1000
PR_COUNT_MULTIPLE_THRESHOLD = 10000
COMMIT_FREQUENCY_MULTIPLE_THRESHOLD = 1000
UPDATED_ISSUES_MULTIPLE_THRESHOLD = 2500
IS_MAINTAINED_MULTIPLE_THRESHOLD = 1

######################### 代码开发效率 #############################################
CODE_REVIEW_RATIO_WEIGHT = 0.1028
CODE_MERGE_RATIO_WEIGHT = 0.1028
COMMIT_PR_LINKED_RATIO_WEIGHT = 0.1561
PR_ISSUE_LINKED_WEIGHT = 0.1164
PR_FIRST_RESPONSE_WEIGHT = -0.1164
PR_OPEN_TIME_WEIGHT = -0.1028
ISSUE_FIRST_RESPONSE_WEIGHT = -0.1164
ISSUE_OPEN_TIME_WEIGHT = -0.1028
COMMENT_FREQUENCY_WEIGHT = 0.0836

CODE_REVIEW_RATIO_THRESHOLD = 1
CODE_MERGE_RATIO_THRESHOLD = 1
COMMIT_PR_LINKED_RATIO_THRESHOLD = 1
PR_ISSUE_LINKED_THRESHOLD = 1
PR_FIRST_RESPONSE_THRESHOLD = 15
PR_OPEN_TIME_THRESHOLD = 30
ISSUE_FIRST_RESPONSE_THRESHOLD = 15
ISSUE_OPEN_TIME_THRESHOLD = 30
COMMENT_FREQUENCY_THRESHOLD = 10

CODE_REVIEW_RATIO_MULTIPLE_THRESHOLD = 1
CODE_MERGE_RATIO_MULTIPLE_THRESHOLD = 1
COMMIT_PR_LINKED_RATIO_MULTIPLE_THRESHOLD = 1
PR_ISSUE_LINKED_MULTIPLE_THRESHOLD = 1
PR_FIRST_RESPONSE_MULTIPLE_THRESHOLD = 15
PR_OPEN_TIME_MULTIPLE_THRESHOLD = 30
ISSUE_FIRST_RESPONSE_MULTIPLE_THRESHOLD = 15
ISSUE_OPEN_TIME_MULTIPLE_THRESHOLD = 30
COMMENT_FREQUENCY_MULTIPLE_THRESHOLD = 10

######################### 代码安全保障#############################################
BUG_ISSUE_OPEN_TIME_WEIGHT = -0.2469
VULNERABILITY_COUNT_WEIGHT = -0.2469
DEFECT_COUNT_WEIGHT = -0.1396
CODE_SMELL_WEIGHT = -0.0756
CODE_CLONE_PERCENT_WEIGHT = -0.0756
CODE_CYCLOMATIC_COMPLEXITY_WEIGHT = -0.0756
LICENSSE_INCLUDE_PERCENT_WEIGHT = 0.1396

BUG_ISSUE_OPEN_TIME_THRESHOLD = 60
VULNERABILITY_COUNT_THRESHOLD = 60
DEFECT_COUNT_THRESHOLD = 60
CODE_SMELL_THRESHOLD = 60
CODE_CLONE_PERCENT_THRESHOLD = 60
CODE_CYCLOMATIC_COMPLEXITY_THRESHOLD = 60
LICENSSE_INCLUDE_PERCENT_THRESHOLD = 1

BUG_ISSUE_OPEN_TIME_MULTIPLE_THRESHOLD = 60
VULNERABILITY_COUNT_MULTIPLE_THRESHOLD = 60
DEFECT_COUNT_MULTIPLE_THRESHOLD = 60
CODE_SMELL_MULTIPLE_THRESHOLD = 60
CODE_CLONE_PERCENT_MULTIPLE_THRESHOLD = 60
CODE_CYCLOMATIC_COMPLEXITY_MULTIPLE_THRESHOLD = 60
LICENSSE_INCLUDE_PERCENT_MULTIPLE_THRESHOLD = 1

######################### 社区活跃度 #############################################
CONTRIBUTORS_NUMBER_WEIGHT = 0.5397
UPDATED_AT_WEIGHT = -0.2970
CREATED_AT_WEIGHT = 0.1634

CONTRIBUTORS_NUMBER_THRESHOLD = 2000
UPDATED_AT_THRESHOLD = 60
CREATED_AT_THRESHOLD = 1000

CONTRIBUTORS_NUMBER_MULTIPLE_THRESHOLD = 2000
UPDATED_AT_MULTIPLE_THRESHOLD = 60
CREATED_AT_MULTIPLE_THRESHOLD = 1000

######################### 人员多样性 #############################################
LOCATION_DISTRIBUTION_WEIGHT = 0.1223
ORGANIZATION_COUNT_WEIGHT = 0.2271
BUS_FACTOR_WEIGHT = 0.4236
ELEPHANT_FACTOR_WEIGHT = 0.2271

LOCATION_DISTRIBUTION_THRESHOLD = 24
ORGANIZATION_COUNT_THRESHOLD = 50
BUS_FACTOR_THRESHOLD = 50
ELEPHANT_FACTOR_THRESHOLD = 50

LOCATION_DISTRIBUTION_MULTIPLE_THRESHOLD = 24
ORGANIZATION_COUNT_MULTIPLE_THRESHOLD = 50
BUS_FACTOR_MULTIPLE_THRESHOLD = 50
ELEPHANT_FACTOR_MULTIPLE_THRESHOLD = 50
######################### 技术多样性 #############################################
LANGUAGE_DISTRIBUTION_WEIGHT = 0.5000
LICENSE_DISTRIBUTION_WEIGHT = 0.5000

LANGUAGE_DISTRIBUTION_THRESHOLD = 50
LICENSE_DISTRIBUTION_THRESHOLD = 20

LANGUAGE_DISTRIBUTION_MULTIPLE_THRESHOLD = 100
LICENSE_DISTRIBUTION_MULTIPLE_THRESHOLD = 20

######################### 总结 #############################################
CODE_DEVELOP_ACTIVITY_SCORE_WEIGHT = 0.1813
CODE_DEVELOP_QUALITY_SCORE_WEIGHT = 0.1813
CODE_SECURITY_SCORE_WEIGHT = 0.3209
COMMUNITY_ACTIVITY_SCORE_WEIGHT = 0.0933
HUMAN_DIVERSITY_SCORE_WEIGHT = 0.1297
TECH_DIVERSITY_SCORE_WEIGHT = 0.0933

CODE_DEVELOP_ACTIVITY_SCORE_THRESHOLD = 100
CODE_DEVELOP_QUALITY_SCORE_THRESHOLD = 100
CODE_SECURITY_SCORE_THRESHOLD = 100
COMMUNITY_ACTIVITY_SCORE_THRESHOLD = 100
HUMAN_DIVERSITY_SCORE_THRESHOLD = 100
TECH_DIVERSITY_SCORE_THRESHOLD = 100

###################################################
CODE_DEVELOP_ACTIVITY_SCORE_MIN = 0
CODE_DEVELOP_QUALITY_SCORE_MIN = -3.5556
CODE_SECURITY_SCORE_MIN = -0.0060
COMMUNITY_ACTIVITY_SCORE_MIN = -0.4061
HUMAN_DIVERSITY_SCORE_MIN = 0
TECH_DIVERSITY_SCORE_MIN = 0
SUMMARY_SCORE_MIN = 0.0444

CODE_DEVELOP_ACTIVITY_SCORE_MAX = 0.9712
CODE_DEVELOP_QUALITY_SCORE_MAX = 1.5810
CODE_SECURITY_SCORE_MAX = 0.7757
COMMUNITY_ACTIVITY_SCORE_MAX = 1.5217
HUMAN_DIVERSITY_SCORE_MAX = 0.6629
TECH_DIVERSITY_SCORE_MAX = 1.0
SUMMARY_SCORE_MAX = 0.2921


DECAY_COEFFICIENT = 0.0027


def get_all_repo(file, source):
    '''Get all repo from json file'''
    all_repo_json = json.load(open(file))
    all_repo = []
    origin = 'gitee' if 'gitee' in source else 'github'
    for i in all_repo_json:
        for j in all_repo_json[i][origin]:
            all_repo.append(j)
    return all_repo

def newest_message(repo_url):
    query = {
        "query": {
            "match": {
                "tag": repo_url
            }
        },
        "sort": [
            {
                "metadata__updated_on": {"order": "desc"}
            }
        ]
    }
    return query

def check_times_has_overlap(dyna_start_time, dyna_end_time, fixed_start_time, fixed_end_time):
    return not (dyna_end_time < fixed_start_time or dyna_start_time > fixed_end_time)

def add_release_message(es_client, out_index, repo_url, releases,):
    item_datas = []
    for item in releases:
        release_data = {
            "_index": out_index,
            "_id": uuid(str(item["id"])),
            "_source": {
                "uuid": uuid(str(item["id"])),
                "id": item["id"],
                "tag": repo_url,
                "tag_name": item["tag_name"],
                "target_commitish": item["target_commitish"],
                "prerelease": item["prerelease"],
                "name": item["name"],
                "author_login": item["author"]["login"],
                "author_name": item["author"]["name"],
                "grimoire_creation_date": item["created_at"],
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
        }
        item_datas.append(release_data)
        if len(item_datas) > MAX_BULK_UPDATE_SIZE:
            helpers.bulk(client=es_client, actions=item_datas)
            item_datas = []
    helpers.bulk(client=es_client, actions=item_datas)


def get_release_index_mapping():
    mapping = {
    "mappings" : {
      "properties" : {
        "author_login" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "author_name" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "body" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "grimoire_creation_date" : {
          "type" : "date"
        },
        "id" : {
          "type" : "long"
        },
        "name" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "prerelease" : {
          "type" : "boolean"
        },
        "tag" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "tag_name" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "target_commitish" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "uuid" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        }
      }
    }
    }
    return mapping


def create_release_index(es_client, all_repo, repo_index, release_index):
    es_exist = es_client.indices.exists(index=release_index)
    if not es_exist:
        res = es_client.indices.create(index=release_index, body=get_release_index_mapping())
    for repo_url in all_repo:
        query = newest_message(repo_url)
        query_hits = es_client.search(index=repo_index, body=query)["hits"]["hits"]
        if len(query_hits) > 0 and query_hits[0]["_source"].get("releases"):
            items = query_hits[0]["_source"]["releases"]
            add_release_message(es_client, release_index, repo_url, items)

def get_all_project(file):
    '''Get all projects from json file'''
    file_json = json.load(open(file))
    all_project = []
    for i in file_json:
        all_project.append(i)
    return all_project


def get_time_diff_months(start, end):
    ''' Number of months between two dates in UTC format  '''

    if start is None or end is None:
        return None

    if type(start) is not datetime:
        start = str_to_datetime(start).replace(tzinfo=None)
    if type(end) is not datetime:
        end = str_to_datetime(end).replace(tzinfo=None)

    seconds_month = float(60 * 60 * 24 * 30)
    diff_months = (end - start).total_seconds() / seconds_month
    diff_months = float('%.2f' % diff_months)

    return diff_months


def get_medium(L):
    L.sort()
    n = len(L)
    m = int(n/2)
    if n == 0:
        return None
    elif n % 2 == 0:
        return (L[m]+L[m-1])/2.0
    else:
        return L[m]


def get_uuid(*args):
    args_list = []
    for arg in args:
        if arg is None or arg == '':
            continue
        args_list.append(arg)
    return uuid(*args_list)

def get_date_list(begin_date, end_date, freq='W-MON'):
    '''Get date list from begin_date to end_date every Monday'''
    date_list = [x for x in list(pd.date_range(freq=freq, start=datetime_to_utc(
        str_to_datetime(begin_date)), end=datetime_to_utc(str_to_datetime(end_date))))]
    return date_list

def normalize(score, min_score, max_score):
    return (score-min_score)/(max_score-min_score)

def get_param_score(param, max_value, weight=1):
    """Return paramater score given its current value, max value and
    parameter weight."""
    if param <= 0:
      return (math.log(1 - param) / math.log(1 + max(-param, max_value))) * -weight
    else:
      return (math.log(1 + param) / math.log(1 + max(param, max_value))) * weight

def get_score_ahp(item, param_dict):
    total_weight = 0
    total_param_score = 0
    for key, value in param_dict.items():
        total_weight += value[0]
        param = 0
        if item[key] is None:
            if value[0] < 0:
                param = value[1]    
        else:
           param = item[key] 
        result = get_param_score(param,value[1] ,value[0])
        total_param_score += result
    try:
        return round(total_param_score / total_weight, 5)
    except ZeroDivisionError:
        return 0.0

def get_codeDevActicity_score(item, level="repo"):
    param_dict = {}
    if level == "community" or level == "project":
        param_dict = {
            "LOC_frequency": [LOC_FREQUENCY_WEIGHT, LOC_FREQUENCY_MULTIPLE_THRESHOLD],
            "contributor_count": [CONTRIBUTOR_COUNT_WEIGHT, CONTRIBUTOR_COUNT_MULTIPLE_THRESHOLD],
            "pr_count": [PR_COUNT_WEIGHT, PR_COUNT_MULTIPLE_THRESHOLD],
            "commit_frequency": [COMMIT_FREQUENCY_WEIGHT, COMMIT_FREQUENCY_MULTIPLE_THRESHOLD],
            "updated_issues_count": [UPDATED_ISSUES_WEIGHT, UPDATED_ISSUES_MULTIPLE_THRESHOLD],
            "is_maintained": [IS_MAINTAINED_WEIGHT, IS_MAINTAINED_MULTIPLE_THRESHOLD],
        }
    if level == "repo":
        param_dict = {
            "LOC_frequency":[LOC_FREQUENCY_WEIGHT, LOC_FREQUENCY_THRESHOLD],
            "contributor_count":[CONTRIBUTOR_COUNT_WEIGHT, CONTRIBUTOR_COUNT_THRESHOLD],
            "pr_count": [PR_COUNT_WEIGHT, PR_COUNT_THRESHOLD],
            "commit_frequency": [COMMIT_FREQUENCY_WEIGHT, COMMIT_FREQUENCY_THRESHOLD],
            "updated_issues_count": [UPDATED_ISSUES_WEIGHT, UPDATED_ISSUES_THRESHOLD],
            "is_maintained": [IS_MAINTAINED_WEIGHT, IS_MAINTAINED_THRESHOLD],
        }
    return normalize(get_score_ahp(item, param_dict), CODE_DEVELOP_ACTIVITY_SCORE_MIN, CODE_DEVELOP_ACTIVITY_SCORE_MAX)

def get_codeDevQuality_score(item, level='repo'):
    param_dict = {}
    if level == "community" or level == "project":
        param_dict = {
          "code_review_ratio": [CODE_REVIEW_RATIO_WEIGHT, CODE_REVIEW_RATIO_MULTIPLE_THRESHOLD],
          "code_merge_ratio": [CODE_MERGE_RATIO_WEIGHT, CODE_MERGE_RATIO_MULTIPLE_THRESHOLD],
          "git_pr_linked_ratio": [COMMIT_PR_LINKED_RATIO_WEIGHT, COMMIT_PR_LINKED_RATIO_MULTIPLE_THRESHOLD],
          "pr_issue_linked_ratio": [PR_ISSUE_LINKED_WEIGHT, PR_ISSUE_LINKED_MULTIPLE_THRESHOLD],
          "pr_first_response_time_avg":[PR_FIRST_RESPONSE_WEIGHT*0.5, PR_FIRST_RESPONSE_MULTIPLE_THRESHOLD],
          "pr_first_response_time_mid":[PR_FIRST_RESPONSE_WEIGHT*0.5, PR_FIRST_RESPONSE_MULTIPLE_THRESHOLD],
          "pr_open_time_avg": [PR_OPEN_TIME_WEIGHT * 0.5, PR_OPEN_TIME_MULTIPLE_THRESHOLD],
          "pr_open_time_mid": [PR_OPEN_TIME_WEIGHT * 0.5, PR_OPEN_TIME_MULTIPLE_THRESHOLD],
          "issue_first_reponse_avg": [ISSUE_FIRST_RESPONSE_WEIGHT * 0.5, ISSUE_FIRST_RESPONSE_MULTIPLE_THRESHOLD],
          "issue_first_reponse_mid": [ISSUE_FIRST_RESPONSE_WEIGHT * 0.5, ISSUE_FIRST_RESPONSE_MULTIPLE_THRESHOLD],
          "issue_open_time_avg": [ISSUE_OPEN_TIME_WEIGHT * 0.5, ISSUE_OPEN_TIME_MULTIPLE_THRESHOLD],
          "issue_open_time_mid": [ISSUE_OPEN_TIME_WEIGHT * 0.5, ISSUE_OPEN_TIME_MULTIPLE_THRESHOLD],
          "comment_frequency": [COMMENT_FREQUENCY_WEIGHT, COMMENT_FREQUENCY_MULTIPLE_THRESHOLD],
        }
    if level == "repo":
        param_dict = {
          "code_review_ratio":[CODE_REVIEW_RATIO_WEIGHT, CODE_REVIEW_RATIO_THRESHOLD],
          "code_merge_ratio":[CODE_MERGE_RATIO_WEIGHT, CODE_MERGE_RATIO_THRESHOLD],
          "git_pr_linked_ratio":[COMMIT_PR_LINKED_RATIO_WEIGHT, COMMIT_PR_LINKED_RATIO_THRESHOLD],
          "pr_issue_linked_ratio":[PR_ISSUE_LINKED_WEIGHT, PR_ISSUE_LINKED_THRESHOLD],
          "pr_first_response_time_avg":[PR_FIRST_RESPONSE_WEIGHT*0.5, PR_FIRST_RESPONSE_THRESHOLD],
          "pr_first_response_time_avg":[PR_FIRST_RESPONSE_WEIGHT*0.5, PR_FIRST_RESPONSE_THRESHOLD],
          "pr_open_time_avg":[PR_OPEN_TIME_WEIGHT*0.5, PR_OPEN_TIME_THRESHOLD],
          "pr_open_time_mid":[PR_OPEN_TIME_WEIGHT*0.5, PR_OPEN_TIME_THRESHOLD],
          "issue_first_reponse_avg":[ISSUE_FIRST_RESPONSE_WEIGHT*0.5, ISSUE_FIRST_RESPONSE_THRESHOLD],
          "issue_first_reponse_mid":[ISSUE_FIRST_RESPONSE_WEIGHT*0.5, ISSUE_FIRST_RESPONSE_THRESHOLD],
          "issue_open_time_avg":[ISSUE_OPEN_TIME_WEIGHT*0.5, ISSUE_OPEN_TIME_THRESHOLD],
          "issue_open_time_mid":[ISSUE_OPEN_TIME_WEIGHT*0.5, ISSUE_OPEN_TIME_THRESHOLD],
          "comment_frequency":[COMMENT_FREQUENCY_WEIGHT, COMMENT_FREQUENCY_THRESHOLD],
        }
    return normalize(get_score_ahp(item, param_dict), CODE_DEVELOP_QUALITY_SCORE_MIN, CODE_DEVELOP_QUALITY_SCORE_MAX)

def get_codeSecurity_score(item, level='repo'):
    param_dict = {}
    if level == "community" or level == "project":
        param_dict = {
            "bug_issue_open_time_avg": [BUG_ISSUE_OPEN_TIME_WEIGHT * 0.5, BUG_ISSUE_OPEN_TIME_MULTIPLE_THRESHOLD],
            "bug_issue_open_time_mid": [BUG_ISSUE_OPEN_TIME_WEIGHT * 0.5, BUG_ISSUE_OPEN_TIME_MULTIPLE_THRESHOLD],
            "vulnerability_count": [VULNERABILITY_COUNT_WEIGHT, VULNERABILITY_COUNT_MULTIPLE_THRESHOLD],
            "defect_count": [DEFECT_COUNT_WEIGHT, DEFECT_COUNT_MULTIPLE_THRESHOLD],
            "code_smell": [CODE_SMELL_WEIGHT, CODE_CLONE_PERCENT_MULTIPLE_THRESHOLD],
            "code_clone_percent": [CODE_CLONE_PERCENT_WEIGHT, CODE_CLONE_PERCENT_MULTIPLE_THRESHOLD],
            "code_cyclomatic_complexity": [CODE_CYCLOMATIC_COMPLEXITY_WEIGHT, CODE_CLONE_PERCENT_MULTIPLE_THRESHOLD],
            "licenses_include_percent": [LICENSSE_INCLUDE_PERCENT_WEIGHT, LICENSSE_INCLUDE_PERCENT_MULTIPLE_THRESHOLD]
        }
    if level == "repo":
        param_dict = {
            "bug_issue_open_time_avg":[BUG_ISSUE_OPEN_TIME_WEIGHT*0.5, BUG_ISSUE_OPEN_TIME_THRESHOLD],
            "bug_issue_open_time_mid":[BUG_ISSUE_OPEN_TIME_WEIGHT*0.5, BUG_ISSUE_OPEN_TIME_THRESHOLD],
            "vulnerability_count": [VULNERABILITY_COUNT_WEIGHT, VULNERABILITY_COUNT_THRESHOLD],
            "defect_count": [DEFECT_COUNT_WEIGHT, DEFECT_COUNT_THRESHOLD],
            "code_smell": [CODE_SMELL_WEIGHT, CODE_CLONE_PERCENT_THRESHOLD],
            "code_clone_percent": [CODE_CLONE_PERCENT_WEIGHT, CODE_CLONE_PERCENT_THRESHOLD],
            "code_cyclomatic_complexity": [CODE_CYCLOMATIC_COMPLEXITY_WEIGHT, CODE_CLONE_PERCENT_THRESHOLD],
            "licenses_include_percent": [LICENSSE_INCLUDE_PERCENT_WEIGHT, LICENSSE_INCLUDE_PERCENT_THRESHOLD]
        }
    return normalize(get_score_ahp(item, param_dict), CODE_SECURITY_SCORE_MIN, CODE_SECURITY_SCORE_MAX)

def get_communityActivity_score(item, level='repo'):
    param_dict = {}
    if level == "community" or level == "project":
        param_dict = {
          "contributors_number": [CONTRIBUTORS_NUMBER_WEIGHT, CONTRIBUTORS_NUMBER_MULTIPLE_THRESHOLD],
          "updated_at": [UPDATED_AT_WEIGHT, UPDATED_AT_MULTIPLE_THRESHOLD],
          "created_at": [CREATED_AT_WEIGHT, CREATED_AT_MULTIPLE_THRESHOLD]
        }
    if level == "repo":
        param_dict = {
          "contributors_number": [CONTRIBUTORS_NUMBER_WEIGHT, CONTRIBUTORS_NUMBER_THRESHOLD],
          "updated_at": [UPDATED_AT_WEIGHT, UPDATED_AT_THRESHOLD],
          "created_at": [CREATED_AT_WEIGHT, CREATED_AT_THRESHOLD]          
        }
    return normalize(get_score_ahp(item, param_dict), COMMUNITY_ACTIVITY_SCORE_MIN, COMMUNITY_ACTIVITY_SCORE_MAX)

def get_humanDiversity_score(item, level='repo'):
    param_dict = {}
    if level == "community" or level == "project":
        param_dict = {
          'location_distribution': [LOCATION_DISTRIBUTION_WEIGHT, LOCATION_DISTRIBUTION_MULTIPLE_THRESHOLD],
          'organization_count': [ORGANIZATION_COUNT_WEIGHT, ORGANIZATION_COUNT_MULTIPLE_THRESHOLD],
          # 'organization_developer_count': [ORGANIZATION_DEVELOPER_COUNT_WEIGHT, ORGANIZATION_DEVELOPER_COUNT_MULTIPLE_THRESHOLD],
          'bus_factor': [BUS_FACTOR_WEIGHT, BUS_FACTOR_MULTIPLE_THRESHOLD],
          'elephant_factor': [ELEPHANT_FACTOR_WEIGHT, ELEPHANT_FACTOR_MULTIPLE_THRESHOLD]
        }
    if level == "repo":
        param_dict = {
          'location_distribution': [LOCATION_DISTRIBUTION_WEIGHT, LOCATION_DISTRIBUTION_THRESHOLD],
          'organization_count': [ORGANIZATION_COUNT_WEIGHT, ORGANIZATION_COUNT_THRESHOLD],
          # 'organization_developer_count': [ORGANIZATION_DEVELOPER_COUNT_WEIGHT, ORGANIZATION_DEVELOPER_COUNT_THRESHOLD],
          'bus_factor': [BUS_FACTOR_WEIGHT, BUS_FACTOR_THRESHOLD],
          'elephant_factor': [ELEPHANT_FACTOR_WEIGHT, ELEPHANT_FACTOR_THRESHOLD]          
        }
    return normalize(get_score_ahp(item, param_dict), HUMAN_DIVERSITY_SCORE_MIN, HUMAN_DIVERSITY_SCORE_MAX)

def get_techDiversity_score(item, level='repo'):
    param_dict = {}
    if level == "community" or level == "project":
        param_dict = {
          'language_distribution': [LANGUAGE_DISTRIBUTION_WEIGHT, LANGUAGE_DISTRIBUTION_MULTIPLE_THRESHOLD],
          'license_distribution': [LICENSE_DISTRIBUTION_WEIGHT, LICENSE_DISTRIBUTION_MULTIPLE_THRESHOLD]
        }
    if level == "repo":
        param_dict = {
          'language_distribution': [LANGUAGE_DISTRIBUTION_WEIGHT, LANGUAGE_DISTRIBUTION_THRESHOLD],
          'license_distribution': [LICENSE_DISTRIBUTION_WEIGHT, LICENSE_DISTRIBUTION_THRESHOLD]
        }
    return normalize(get_score_ahp(item, param_dict), TECH_DIVERSITY_SCORE_MIN, TECH_DIVERSITY_SCORE_MAX)

def get_summary_score(item, level='repo'):
    param_dict = {
      'copy_code_develop_activity_score': [CODE_DEVELOP_ACTIVITY_SCORE_WEIGHT, CODE_DEVELOP_ACTIVITY_SCORE_WEIGHT],
      'copy_code_develop_quality_score': [CODE_DEVELOP_QUALITY_SCORE_WEIGHT, CODE_DEVELOP_QUALITY_SCORE_THRESHOLD],
      'copy_code_security_score': [CODE_SECURITY_SCORE_WEIGHT, CODE_SECURITY_SCORE_THRESHOLD],
      'copy_community_activity_score': [COMMUNITY_ACTIVITY_SCORE_WEIGHT, COMMUNITY_ACTIVITY_SCORE_THRESHOLD],
      'copy_human_diversity_score': [HUMAN_DIVERSITY_SCORE_WEIGHT, HUMAN_DIVERSITY_SCORE_THRESHOLD],
      'copy_tech_diversity_score': [TECH_DIVERSITY_SCORE_WEIGHT, TECH_DIVERSITY_SCORE_THRESHOLD]
    }
    return normalize(get_score_ahp(item, param_dict), SUMMARY_SCORE_MIN, SUMMARY_SCORE_MAX)

def increment_decay(last_data, threshold, days):
    return min(last_data + DECAY_COEFFICIENT * threshold * days, threshold)

def decrease_decay(last_data, threshold, days):
    return max(last_data - DECAY_COEFFICIENT * threshold * days, 0)

def codeDevQuality_decay(item, last_data, level="repo"):
    if last_data == None:
        return item
    decay_item = item.copy()
    increment_decay_dict = {}
    decrease_decay_dict = {}
    if level == "community" or level == "project":
        increment_decay_dict = {
            "issue_first_reponse_avg": ISSUE_FIRST_RESPONSE_MULTIPLE_THRESHOLD,
            "issue_first_reponse_mid": ISSUE_FIRST_RESPONSE_MULTIPLE_THRESHOLD,
            "pr_open_time_avg": PR_OPEN_TIME_MULTIPLE_THRESHOLD,
            "pr_open_time_mid": PR_OPEN_TIME_MULTIPLE_THRESHOLD,
            "pr_first_response_time_avg": PR_FIRST_RESPONSE_MULTIPLE_THRESHOLD,
            "pr_first_response_time_mid": PR_FIRST_RESPONSE_MULTIPLE_THRESHOLD,

        }        
        decrease_decay_dict = {
            "code_merge_ratio": CODE_MERGE_RATIO_MULTIPLE_THRESHOLD,
            "code_review_ratio": CODE_REVIEW_RATIO_MULTIPLE_THRESHOLD,
            "pr_issue_linked_ratio": PR_ISSUE_LINKED_MULTIPLE_THRESHOLD,
            "git_pr_linked_ratio": COMMIT_PR_LINKED_RATIO_MULTIPLE_THRESHOLD,
            "comment_frequency": COMMENT_FREQUENCY_MULTIPLE_THRESHOLD,
        }
    if level == "repo":
        increment_decay_dict = {
            "issue_first_reponse_avg":ISSUE_FIRST_RESPONSE_THRESHOLD,
            "issue_first_reponse_mid":ISSUE_FIRST_RESPONSE_THRESHOLD,
            "pr_open_time_avg":PR_OPEN_TIME_THRESHOLD,
            "pr_open_time_mid":PR_OPEN_TIME_THRESHOLD,
            "pr_first_response_time_avg": PR_FIRST_RESPONSE_THRESHOLD,
            "pr_first_response_time_mid": PR_FIRST_RESPONSE_THRESHOLD,
            }
        decrease_decay_dict = {
            "code_merge_ratio": CODE_MERGE_RATIO_THRESHOLD,
            "code_review_ratio":CODE_REVIEW_RATIO_THRESHOLD,
            "pr_issue_linked_ratio":PR_ISSUE_LINKED_THRESHOLD,
            "git_pr_linked_ratio":COMMIT_PR_LINKED_RATIO_THRESHOLD,
            "comment_frequency":COMMENT_FREQUENCY_THRESHOLD,
            }

    for key, value in increment_decay_dict.items():
        if item[key] == None and last_data.get(key) != None:
            days = pendulum.parse(item['grimoire_creation_date']).diff(pendulum.parse(last_data[key][1])).days
            decay_item[key] = round(increment_decay(last_data[key][0], value, days), 4)
    for key, value in decrease_decay_dict.items():
        if item[key] == None and last_data.get(key) != None:
            days = pendulum.parse(item['grimoire_creation_date']).diff(pendulum.parse(last_data[key][1])).days
            decay_item[key] = round(decrease_decay(last_data[key][0], value, days), 4)
    return decay_item