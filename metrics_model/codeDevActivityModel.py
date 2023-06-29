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
                    get_codeDevActicity_score
                    )

import logging
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)
MAX_BULK_UPDATE_SIZE = 500
class CodeDevActivityModel(MetricsModel):
    def __init__(self, issue_index=None, pr_index=None, repo_index=None, json_file=None, git_index=None, out_index=None, git_branch=None, from_date=None, end_date=None, community=None, level=None, company=None, pr_comments_index=None):
        super().__init__(json_file, from_date, end_date, out_index, community, level)
        self.git_index = git_index
        self.issue_index = issue_index
        self.repo_index = repo_index
        self.git_branch = git_branch
        self.pr_index = pr_index
        self.pr_comments_index = pr_comments_index
        self.model_name = 'Code develop activity'
        self.company = None if company == None or company == 'None' else company

    def LOC_frequency(self, date, repos_list, field='lines_changed'):
        query_LOC_frequency = self.get_uuid_count_query(
            'sum', repos_list, field, 'grimoire_creation_date', size=0, from_date=date-timedelta(days=90), to_date=date)
        # print("query_LOC_frequency", query_LOC_frequency)
        LOC_frequency = self.es_in.search(index=self.git_index, body=query_LOC_frequency)[
            'aggregations']['count_of_uuid']['value']
        return LOC_frequency/12.85

    def contributor_count(self, date, repos_list):
        query_author_uuid_data = self.get_uuid_count_contribute_query(
            repos_list, company=None, from_date=(date - timedelta(days=90)), to_date=date)
        author_uuid_count = self.es_in.search(index=(self.git_index, self.pr_comments_index), body=query_author_uuid_data)[
            'aggregations']["count_of_contributors"]['value']
        return author_uuid_count

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

    def commit_frequency(self, date, repos_list):
        query_commit_frequency = self.get_uuid_count_query(
            "cardinality", repos_list, "hash", "grimoire_creation_date", size=0, from_date=date - timedelta(days=90), to_date=date)
        commit_frequency = self.es_in.search(index=self.git_index, body=query_commit_frequency)[
            'aggregations']["count_of_uuid"]['value']
        query_commit_frequency_commpany = 0
        if self.company:
            query_commit_frequency["query"]["bool"]["must"].append({ "match": { "author_org_name": self.company } })
            query_commit_frequency_commpany = self.es_in.search(index=self.git_index, body=query_commit_frequency)[
                'aggregations']["count_of_uuid"]['value']
        return commit_frequency/12.85, query_commit_frequency_commpany/12.85

    def updated_issue_count(self, date, repos_list):
        query_issue_updated_since = self.get_uuid_count_query(
            "cardinality", repos_list, "uuid", date_field='metadata__updated_on', size=0, from_date=(date-timedelta(days=90)), to_date=date)
        query_issue_updated_since["query"]["bool"]["must"].append({"match_phrase": {"pull_request": "false" }})
        updated_issues_count = self.es_in.search(index=self.issue_index, body=query_issue_updated_since)[
            'aggregations']["count_of_uuid"]['value']
        return updated_issues_count

    def is_maintained(self, date, repos_list, level):
        is_maintained_list = []
        if level == "repo":
            date_list_maintained = get_date_list(begin_date=str(
                date-timedelta(days=90)), end_date=str(date), freq='7D')
            for day in date_list_maintained:
                query_git_commit_i = self.get_uuid_count_query(
                    "cardinality", repos_list, "hash", size=0, from_date=day-timedelta(days=7), to_date=day)
                commit_frequency_i = self.es_in.search(index=self.git_index, body=query_git_commit_i)[
                    'aggregations']["count_of_uuid"]['value']
                if commit_frequency_i > 0:
                    is_maintained_list.append("True")
                else:
                    is_maintained_list.append("False")

        elif level in ["project", "community"]:
            for repo in repos_list:
                query_git_commit_i = self.get_uuid_count_query("cardinality",[repo+'.git'], "hash",from_date=date-timedelta(days=30), to_date=date)
                commit_frequency_i = self.es_in.search(index=self.git_index, body=query_git_commit_i)['aggregations']["count_of_uuid"]['value']
                if commit_frequency_i > 0:
                    is_maintained_list.append("True")
                else:
                    is_maintained_list.append("False")
        try:
            return is_maintained_list.count("True") / len(is_maintained_list)
        except ZeroDivisionError:
            return 0


    def metrics_model_enrich(self, repos_list, label, type=None, level=None, date_list=None):
        level = level if level is not None else self.level
        date_list = date_list if date_list is not None else self.date_list
        item_datas = []
        last_metrics_data = {}
        for date in date_list:
            print(str(date)+"--"+self.model_name+"--"+label)
            created_since = self.created_since(date, repos_list)
            if created_since is None:
                continue
            LOC_frequency = self.LOC_frequency(date, repos_list)
            code_review_ratio, pr_count = self.code_review_ratio(date, repos_list)
            commit_frequency_message = self.commit_frequency(date, repos_list)
            metrics_data = {
                'uuid': get_uuid(str(date), self.community, level, label, self.model_name, type),
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                'LOC_frequency': LOC_frequency,
                'contributor_count': self.contributor_count(date, repos_list),
                'pr_count': pr_count,
                'commit_frequency': commit_frequency_message[0],
                'updated_issues_count': self.updated_issue_count(date, repos_list),
                'is_maintained': round(self.is_maintained(date, repos_list, level), 4),
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
            score = get_codeDevActicity_score(metrics_data, level)
            metrics_data["code_develop_activity_score"] = score
            print(score)
            item_datas.append(metrics_data)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                self.es_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")
