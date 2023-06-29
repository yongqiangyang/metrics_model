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
                    get_codeSecurity_score,
                    codeDevQuality_decay
                    )

import logging
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)
from grimoire_elk.enriched.utils import get_time_diff_days
MAX_BULK_UPDATE_SIZE = 500
import random
class CodeSecurityModel(MetricsModel):
    def __init__(self, issue_index=None, pr_index=None, repo_index=None, json_file=None, git_index=None, cocom_index=None, colic_index=None, out_index=None, git_branch=None, from_date=None, end_date=None, community=None, level=None, company=None, pr_comments_index=None):
        super().__init__(json_file, from_date, end_date, out_index, community, level)
        self.git_index = git_index
        self.issue_index = issue_index
        self.repo_index = repo_index
        self.git_branch = git_branch
        self.pr_index = pr_index
        self.cocom_index = cocom_index
        self.colic_index = colic_index
        self.pr_comments_index = pr_comments_index
        self.model_name = 'Code security'
        self.company = None if company == None or company == 'None' else company
    
    def bug_issue_open_time(self, date, repos_list):
        query_issue_opens = self.get_uuid_count_query("avg", repos_list, "time_to_first_attention_without_bot",
                                                      "grimoire_creation_date", size=10000, from_date=date-timedelta(days=90), to_date=date)
        query_issue_opens["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "false" }})
        bug_query = {
            "bool": {
                "should": [{
                    "script": {
                        "script": "if (doc.containsKey('labels') && doc['labels'].size()>0) { for (int i = 0; i < doc['labels'].length; ++i){ if(doc['labels'][i].toLowerCase().indexOf('bug')!=-1|| doc['labels'][i].toLowerCase().indexOf('缺陷')!=-1){return true;}}}"
                    }
                },
                    {
                    "script": {
                        "script": "if (doc.containsKey('issue_type') && doc['issue_type'].size()>0) { for (int i = 0; i < doc['issue_type'].length; ++i){ if(doc['issue_type'][i].toLowerCase().indexOf('bug')!=-1 || doc['issue_type'][i].toLowerCase().indexOf('缺陷')!=-1){return true;}}}"
                    }
                }],
                "minimum_should_match": 1
            }
        }
        query_issue_opens["query"]["bool"]["must"].append(bug_query)
        issue_opens_items = self.es_in.search(
            index=self.issue_index, body=query_issue_opens)['hits']['hits']
        if len(issue_opens_items) == 0:
            return None, None
        issue_open_time_repo = []
        for item in issue_opens_items:
            if 'state' in item['_source']:
                if item['_source']['closed_at'] and item['_source']['state'] in ['closed', 'rejected'] and str_to_datetime(item['_source']['closed_at']) < date:
                        issue_open_time_repo.append(get_time_diff_days(
                            item['_source']['created_at'], item['_source']['closed_at']))
                else:
                    issue_open_time_repo.append(get_time_diff_days(
                        item['_source']['created_at'], str(date)))
        issue_open_time_repo_avg = sum(issue_open_time_repo)/len(issue_open_time_repo)
        issue_open_time_repo_mid = get_medium(issue_open_time_repo)
        return issue_open_time_repo_avg, issue_open_time_repo_mid

    def code_cyclomatic_complexity(self, date, repos_list):
        query = self.get_uuid_count_query('sum', repos_list, 'ccn', 'grimoire_creation_date', size=0, from_date=date-timedelta(days=90), to_date=date)
        code_cyclomatic_complexity = self.es_in.search(index=self.cocom_index, body=query)[
            'aggregations']['count_of_uuid']['value']
        return code_cyclomatic_complexity/12.85
    
    def licenses_include_percent(self, date, repos_list):
        query = self.get_uuid_count_query('sum', repos_list, 'has_license', 'grimoire_creation_date', size=0, from_date=date-timedelta(days=90), to_date=date)
        answer = self.es_in.search(index=self.colic_index, body=query)
        has_license_count = answer['aggregations']['count_of_uuid']['value']
        total_files = answer['hits']['total'] if answer['hits']['total'] is not 0 else 1
        return has_license_count / total_files

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
            bug_issue_open_time = self.bug_issue_open_time(date, repos_list)
            vulnerability_count = self.vulnerability_count(date, repos_list)
            defect_count = self.defect_count(date, repos_list)
            code_smell = self.code_smell(date, repos_list)
            code_clone_percent = self.code_clone_percent(date, repos_list)
            code_cyclomatic_complexity = self.code_cyclomatic_complexity(date, repos_list)
            # print("code_cyclomatic_complexity", code_cyclomatic_complexity)
            licenses_include_percent = self.licenses_include_percent(date, repos_list)
            # print("licenses_include_percent", licenses_include_percent)
            metrics_data = {
                'uuid': get_uuid(str(date), self.community, level, label, self.model_name, type),
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                'bug_issue_open_time_avg': round(bug_issue_open_time[0], 4) if bug_issue_open_time[0] is not None else None,
                'bug_issue_open_time_mid': round(bug_issue_open_time[1], 4) if bug_issue_open_time[1] is not None else None,
                'vulnerability_count' : vulnerability_count,
                'defect_count': defect_count,
                'code_smell': code_smell,
                'code_clone_percent': code_clone_percent,
                'code_cyclomatic_complexity': code_cyclomatic_complexity,
                'licenses_include_percent': licenses_include_percent,
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
            # TODO
            score = get_codeSecurity_score(metrics_data, level)
            metrics_data["code_security_score"] = score
            item_datas.append(metrics_data)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                self.es_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")
