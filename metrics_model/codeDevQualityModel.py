from .metrics_model import MetricsModel

from .utils import (get_all_repo,
                    newest_message,
                    check_times_has_overlap,
                    add_release_message,
                    get_release_index_mapping,
                    create_release_index,
                    get_all_project,
                    get_time_diff_months,
                    get_medium,
                    get_uuid,
                    get_date_list,
                    get_codeDevActicity_score,
                    get_codeDevQuality_score,
                    codeDevQuality_decay
                    )

import logging
import math
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)
from grimoire_elk.enriched.utils import get_time_diff_days
MAX_BULK_UPDATE_SIZE = 500

class CodeDevQualityModel(MetricsModel):
    def __init__(self, issue_index=None, pr_index=None, repo_index=None, json_file=None, git_index=None, out_index=None, git_branch=None, from_date=None, end_date=None, community=None, level=None, company=None, pr_comments_index=None):
        super().__init__(json_file, from_date, end_date, out_index, community, level)
        self.git_index = git_index
        self.issue_index = issue_index
        self.repo_index = repo_index
        self.git_branch = git_branch
        self.pr_index = pr_index
        self.pr_comments_index = pr_comments_index
        self.model_name = 'Code develop Quality'
        self.company = None if company == None or company == 'None' else company

    def get_pr_message_count(self, repos_list, field, date_field="grimoire_creation_date", size=0, filter_field=None, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query = {
            "size": size,
            "track_total_hits": True,
            "aggs": {
                "count_of_uuid": {
                    "cardinality": {
                        "field": field
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [{
                                    "simple_query_string": {
                                        "query": i,
                                        "fields": ["tag"]
                                    }}for i in repos_list],
                                "minimum_should_match": 1
                            }
                        },
                        {
                            "match_phrase": {
                                "pull_request": "true"
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range":
                            {
                                filter_field: {
                                    "gte": 1
                                }
                            }},
                        {
                            "range":
                            {
                                date_field: {
                                    "gte": from_date.strftime("%Y-%m-%d"),
                                    "lt": to_date.strftime("%Y-%m-%d")
                                }
                            }
                        }
                    ]
                }
            }
        }
        return query

    def get_pr_linked_issue_count(self, repo, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query = {
            "size": 0,
            "track_total_hits": True,
            "aggs": {
                "count_of_uuid": {
                    "cardinality": {
                        "script": "if(doc.containsKey('pull_id')) {return doc['pull_id']} else {return doc['id']}"
                    }
                }
            },
            "query": {
                "bool": {
                    "should": [
                        {
                            "range": {
                                "linked_issues_count": {
                                    "gte": 1
                                }
                            }
                        },
                        {
                            "script": {
                                "script": "if (doc.containsKey('body') && doc['body'].size()>0 &&doc['body'].value.indexOf('"+repo+"/issue') != -1){return true}"
                            }
                        }
                    ],
                    "minimum_should_match": 1,
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {
                                        "simple_query_string": {
                                            "query": repo,
                                            "fields": [
                                                "tag"
                                            ]
                                        }
                                    }
                                ],
                                "minimum_should_match": 1
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "grimoire_creation_date": {
                                    "gte": from_date.strftime("%Y-%m-%d"),
                                    "lt": to_date.strftime("%Y-%m-%d")
                                }
                            }
                        }
                    ]
                }
            }
        }
        return query

    def code_review_ratio(self, date, repos_list):
        query_pr_count = self.get_uuid_count_query(
            "cardinality", repos_list, "uuid", size=0, from_date=(date-timedelta(days=90)), to_date=date)
        pr_count = self.es_in.search(index=self.pr_index, body=query_pr_count)[
            'aggregations']["count_of_uuid"]['value']
        query_pr_body = self.get_pr_message_count(repos_list, "uuid", "grimoire_creation_date", size=0,
                                                  filter_field="num_review_comments_without_bot", from_date=(date-timedelta(days=90)), to_date=date)
        prs = self.es_in.search(index=self.pr_index, body=query_pr_body)[
            'aggregations']["count_of_uuid"]['value']
        try:
            return prs/pr_count, pr_count
        except ZeroDivisionError:
            return None, 0

    def code_merge_ratio(self, date, repos_list):
        query_pr_body = self.get_uuid_count_query( "cardinality", repos_list, "uuid", "grimoire_creation_date", size=0, from_date=(date-timedelta(days=90)), to_date=date)
        query_pr_body["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "true" }})
        query_pr_body["query"]["bool"]["must"].append({"match_phrase": {"merged": "true" }})
        pr_merged_count = self.es_in.search(index=self.pr_index, body=query_pr_body)[
            'aggregations']["count_of_uuid"]['value']
        query_pr_body["query"]["bool"]["must"].append({
                            "script": {
                                "script": "if(doc['merged_by_data_name'].size() > 0 && doc['author_name'].size() > 0 && doc['merged_by_data_name'].value !=  doc['author_name'].value){return true}"
                            }
                        })
        prs = self.es_in.search(index=self.pr_index, body=query_pr_body)[
            'aggregations']["count_of_uuid"]['value']
        try:
            return prs/pr_merged_count, pr_merged_count
        except ZeroDivisionError:
            return None, 0

    def git_pr_linked_ratio(self, date, repos_list):
        commit_frequency = self.get_uuid_count_query("cardinality", repos_list, "hash", "grimoire_creation_date", size=10000, from_date=date - timedelta(days=90), to_date=date)
        commits_without_merge_pr = {
            "bool": {
                "should": [{"script": {
                    "script": "if (doc.containsKey('message') && doc['message'].size()>0 &&doc['message'].value.indexOf('Merge pull request') == -1){return true}"
                }
                }],
                "minimum_should_match": 1}
        }
        commit_frequency["query"]["bool"]["must"].append(commits_without_merge_pr)
        commit_message = self.es_in.search(index=self.git_index, body=commit_frequency)
        commit_count = commit_message['aggregations']["count_of_uuid"]['value']
        commit_pr_cout = 0
        commit_all_message = [commit_message_i['_source']['hash']  for commit_message_i in commit_message['hits']['hits']]

        for commit_message_i in set(commit_all_message):
            commit_hash = commit_message_i
            if commit_hash in self.commit_message_dict:
                commit_pr_cout += self.commit_message_dict[commit_hash]
            else:
                pr_message = self.get_uuid_count_query("cardinality", repos_list, "uuid", "grimoire_creation_date", size=0)
                commit_hash_query = { "bool": {"should": [ {"match_phrase": {"commits_data": commit_hash} }],
                                        "minimum_should_match": 1
                                    }
                                }
                pr_message["query"]["bool"]["must"].append(commit_hash_query)
                prs = self.es_in.search(index=self.pr_index, body=pr_message)
                if prs['aggregations']["count_of_uuid"]['value']>0:
                    self.commit_message_dict[commit_hash] = 1
                    commit_pr_cout += 1
                else:
                    self.commit_message_dict[commit_hash] = 0
        if commit_count>0:
            return len(commit_all_message), commit_pr_cout, commit_pr_cout/len(commit_all_message)
        else:
            return 0, None, None

    def pr_issue_linked(self, date, repos_list):
        pr_linked_issue = 0
        for repo in repos_list:
            query_pr_linked_issue = self.get_pr_linked_issue_count(
                repo, from_date=date-timedelta(days=90), to_date=date)
            pr_linked_issue += self.es_in.search(index=(self.pr_index, self.pr_comments_index), body=query_pr_linked_issue)[
                'aggregations']["count_of_uuid"]['value']
        query_pr_count = self.get_uuid_count_query(
            "cardinality", repos_list, "uuid", size=0, from_date=(date-timedelta(days=90)), to_date=date)
        query_pr_count["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "true" }})
        pr_count = self.es_in.search(index=self.pr_index,
                                    body=query_pr_count)[
            'aggregations']["count_of_uuid"]['value']
        try:
            return pr_linked_issue/pr_count
        except ZeroDivisionError:
            return None

    def pr_first_response_time(self, date, repos_list):
        query_pr_first_reponse_avg = self.get_uuid_count_query(
            "avg", repos_list, "time_to_first_attention_without_bot", "grimoire_creation_date", size=0, from_date=date-timedelta(days=90), to_date=date)
        query_pr_first_reponse_avg["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "true" }})
        pr_first_reponse = self.es_in.search(index=self.pr_index, body=query_pr_first_reponse_avg)
        if pr_first_reponse["hits"]["total"] == 0:
            return None, None
        pr_first_reponse_avg = pr_first_reponse['aggregations']["count_of_uuid"]['value']
        query_pr_first_reponse_mid = self.get_uuid_count_query(
            "percentiles", repos_list, "time_to_first_attention_without_bot", "grimoire_creation_date", size=0, from_date=date-timedelta(days=90), to_date=date)
        query_pr_first_reponse_mid["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "true" }})
        query_pr_first_reponse_mid["aggs"]["count_of_uuid"]["percentiles"]["percents"] = [
            50]
        pr_first_reponse_mid = self.es_in.search(index=self.pr_index, body=query_pr_first_reponse_mid)[
            'aggregations']["count_of_uuid"]['values']['50.0']
        pr_first_reponse_mid = pr_first_reponse_mid if pr_first_reponse_mid != 'NaN' else None
        return pr_first_reponse_avg, pr_first_reponse_mid

    def pr_open_time(self, date, repos_list):
        query_pr_opens = self.get_uuid_count_query("avg", repos_list, "time_to_first_attention_without_bot",
                                                   "grimoire_creation_date", size=10000, from_date=date-timedelta(days=90), to_date=date)
        query_pr_opens["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "true" }})
        pr_opens_items = self.es_in.search(
            index=self.pr_index, body=query_pr_opens)['hits']['hits']
        if len(pr_opens_items) == 0:
            return None, None
        pr_open_time_repo = []
        for item in pr_opens_items:
            if 'state' in item['_source']:
                if item['_source']['state'] == 'merged' and item['_source']['merged_at'] and str_to_datetime(item['_source']['merged_at']) < date:
                    pr_open_time_repo.append(get_time_diff_days(
                        item['_source']['created_at'], item['_source']['merged_at']))
                if item['_source']['state'] == 'closed' and str_to_datetime(item['_source']['closed_at'] or item['_source']['updated_at']) < date:
                    pr_open_time_repo.append(get_time_diff_days(
                        item['_source']['created_at'], item['_source']['closed_at'] or item['_source']['updated_at']))
                else:
                    pr_open_time_repo.append(get_time_diff_days(
                        item['_source']['created_at'], str(date)))
        if len(pr_open_time_repo) == 0:
            return None, None
        pr_open_time_repo_avg = float(sum(pr_open_time_repo)/len(pr_open_time_repo))
        pr_open_time_repo_mid = get_medium(pr_open_time_repo)
        return pr_open_time_repo_avg, pr_open_time_repo_mid

    def issue_first_reponse(self, date, repos_list):
        query_issue_first_reponse_avg = self.get_uuid_count_query(
            "avg", repos_list, "time_to_first_attention_without_bot", "grimoire_creation_date", size=0, from_date=date-timedelta(days=90), to_date=date)
        query_issue_first_reponse_avg["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "false" }})
        issue_first_reponse = self.es_in.search(index=self.issue_index, body=query_issue_first_reponse_avg)
        if issue_first_reponse["hits"]["total"] == 0:
            return None, None
        issue_first_reponse_avg = issue_first_reponse['aggregations']["count_of_uuid"]['value']
        query_issue_first_reponse_mid = self.get_uuid_count_query(
            "percentiles", repos_list, "time_to_first_attention_without_bot", "grimoire_creation_date", size=0, from_date=date-timedelta(days=90), to_date=date)
        query_issue_first_reponse_mid["aggs"]["count_of_uuid"]["percentiles"]["percents"] = [
            50]
        query_issue_first_reponse_mid["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "false" }})
        issue_first_reponse_mid = self.es_in.search(index=self.issue_index, body=query_issue_first_reponse_mid)[
            'aggregations']["count_of_uuid"]['values']['50.0']
        issue_first_reponse_mid = issue_first_reponse_mid if not isinstance(issue_first_reponse_mid, str) else None
        return issue_first_reponse_avg, issue_first_reponse_mid

    def issue_open_time(self, date, repos_list):
        query_issue_opens = self.get_uuid_count_query("avg", repos_list, "time_to_first_attention_without_bot", "grimoire_creation_date", size=10000, from_date=date-timedelta(days=90), to_date=date)
        query_issue_opens["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "false" }})
        issue_opens_items = self.es_in.search(index=self.issue_index, body=query_issue_opens)['hits']['hits']
        if len(issue_opens_items) == 0:
            return None, None
        issue_open_time_repo = []
        for item in issue_opens_items:
            if 'state' in item['_source']:
                if item['_source']['closed_at']:
                    if item['_source']['state'] in ['closed', 'rejected'] and str_to_datetime(item['_source']['closed_at']) < date:
                        issue_open_time_repo.append(get_time_diff_days(
                            item['_source']['created_at'], item['_source']['closed_at']))
                else:
                    issue_open_time_repo.append(get_time_diff_days(
                        item['_source']['created_at'], str(date)))
        if len(issue_open_time_repo) == 0:
            return None, None
        issue_open_time_repo_avg = sum(issue_open_time_repo)/len(issue_open_time_repo)
        issue_open_time_repo_mid = get_medium(issue_open_time_repo)
        return issue_open_time_repo_avg, issue_open_time_repo_mid

    def comment_frequency(self, date, repos_list):
        query_issue_comments_count = self.get_uuid_count_query(
            "sum", repos_list, "num_of_comments_without_bot", date_field='grimoire_creation_date', size=0, from_date=(date-timedelta(days=90)), to_date=date)
        query_issue_comments_count["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "false" }})
        issue = self.es_in.search(
            index=self.issue_index, body=query_issue_comments_count)
        try:
            return float(issue['aggregations']["count_of_uuid"]['value']/issue["hits"]["total"])
        except ZeroDivisionError:
            return None

    def metrics_model_enrich(self, repos_list, label, type=None, level=None, date_list=None):
        level = level if level is not None else self.level
        date_list = date_list if date_list is not None else self.date_list
        item_datas = []
        last_metrics_data = {}
        self.commit_message_dict = {}
        for date in date_list:
            print(str(date)+"--"+self.model_name+"--"+label)
            created_since = self.created_since(date, repos_list)
            if created_since is None:
                continue
            code_review_ratio, pr_count = self.code_review_ratio(date, repos_list)
            # print("code_review_ratio", code_review_ratio)
            code_merge_ratio, pr_merged_count = self.code_merge_ratio(date, repos_list)
            # print("code_merge_ratio", code_merge_ratio)
            git_pr_linked_ratio = self.git_pr_linked_ratio(date, repos_list)
            # print("git_pr_linked_ratio", git_pr_linked_ratio)
            pr_issue_linked_ratio = self.pr_issue_linked(date, repos_list)
            # print("pr_issue_linked_ratio", pr_issue_linked_ratio)
            pr_first_response_time = self.pr_first_response_time(date, repos_list)
            # print("pr_first_response_time", pr_first_response_time)
            pr_open_time = self.pr_open_time(date, repos_list)
            issue_first = self.issue_first_reponse(date, repos_list)
            # print(issue_first[0], issue_first[1])
            issue_open_time = self.issue_open_time(date, repos_list)
            comment_frequency = self.comment_frequency(date, repos_list)
            metrics_data = {
                'uuid': get_uuid(str(date), self.community, level, label, self.model_name, type),
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                'code_review_ratio': code_review_ratio,
                'code_merge_ratio': code_merge_ratio,
                'git_pr_linked_ratio': git_pr_linked_ratio[2],
                'pr_issue_linked_ratio': pr_issue_linked_ratio,
                'pr_first_response_time_avg': round(pr_first_response_time[0], 4) if pr_first_response_time[0] is not None else None,
                'pr_first_response_time_mid': round(pr_first_response_time[1], 4) if pr_first_response_time[1] is not None else None,
                'pr_open_time_avg': round(pr_open_time[0], 4) if pr_open_time[0] is not None else None,
                'pr_open_time_mid': round(pr_open_time[1], 4) if pr_open_time[1] is not None else None,
                'issue_first_reponse_avg': round(issue_first[0], 4) if issue_first[0] is not None else None,
                'issue_first_reponse_mid': round(issue_first[1], 4) if issue_first[1] is not None else None,
                'issue_open_time_avg': round(issue_open_time[0], 4) if issue_open_time[0] is not None else None,
                'issue_open_time_mid': round(issue_open_time[1], 4) if issue_open_time[1] is not None else None,
                'comment_frequency': float(round(comment_frequency, 4)) if comment_frequency is not None else None,
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
            # TODO
            self.cache_last_metrics_data(metrics_data, last_metrics_data)
            score = get_codeDevQuality_score(codeDevQuality_decay(metrics_data, last_metrics_data, level), level)
            metrics_data["code_develop_quality_score"] = score
            item_datas.append(metrics_data)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                self.es_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")
    
    def cache_last_metrics_data(self, item, last_metrics_data):
        for i in ["issue_first_reponse_avg",  "issue_first_reponse_mid",
                    "pr_open_time_avg","pr_open_time_mid",
                    "pr_first_response_time_avg", "pr_first_response_time_mid",
                    "comment_frequency", "code_merge_ratio",  "code_review_ratio",
                    "pr_issue_linked_ratio", "git_pr_linked_ratio"]:
            if item[i] != None:
                data = [item[i],item['grimoire_creation_date']]
                last_metrics_data[i] = data
