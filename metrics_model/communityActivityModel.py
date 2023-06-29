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
                    get_communityActivity_score,
                    codeDevQuality_decay
                    )

import logging
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)
from grimoire_elk.enriched.utils import get_time_diff_days
MAX_BULK_UPDATE_SIZE = 500

class CommunityActivityModel(MetricsModel):
    def __init__(self, issue_index=None, pr_index=None, repo_index=None, json_file=None, git_index=None, issue_comments_index=None, out_index=None, git_branch=None, from_date=None, end_date=None, community=None, level=None, company=None, pr_comments_index=None):
        super().__init__(json_file, from_date, end_date, out_index, community, level)
        self.git_index = git_index
        self.issue_index = issue_index
        self.repo_index = repo_index
        self.git_branch = git_branch
        self.pr_index = pr_index
        self.issue_comments_index = issue_comments_index
        self.pr_comments_index = pr_comments_index
        self.model_name = 'community activity'
        self.company = None if company == None or company == 'None' else company

    def contributors_number(self, date, repos_list):
        query_author_uuid_data = self.get_uuid_count_contribute_query(
            repos_list, company=None, from_date=(date - timedelta(days=90)), to_date=date)
        author_uuid_count = self.es_in.search(index=(self.git_index, self.issue_index, self.pr_index, self.issue_comments_index,self.pr_comments_index), body=query_author_uuid_data)[
            'aggregations']["count_of_contributors"]['value']
        return author_uuid_count

    def updated_since(self, date, repos_list):
        updated_since_list = []
        for repo in repos_list:
            query_updated_since = self.get_updated_since_query(
                [repo], date_field='metadata__updated_on', to_date=date)
            updated_since = self.es_in.search(
                index=self.git_index, body=query_updated_since)['hits']['hits']
            if updated_since:
                updated_since_list.append(get_time_diff_months(
                    updated_since[0]['_source']["metadata__updated_on"], str(date)))
        if updated_since_list:
            return sum(updated_since_list) / len(updated_since_list)
        else:
            return 0

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
            contributors_number = self.contributors_number(date, repos_list)
            # print("contributors_number", contributors_number)
            updated_at = self.updated_since(date, repos_list)
            print("updated_at", updated_at)
            print("created_since", created_since)
            metrics_data = {
                'uuid': get_uuid(str(date), self.community, level, label, self.model_name, type),
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                'contributors_number': contributors_number,
                'updated_at': float(round(updated_at, 4)),
                'created_at': float(round(created_since, 4)),
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
            # TODO
            score = get_communityActivity_score(metrics_data, level)
            metrics_data["community_activity_score"] = score
            item_datas.append(metrics_data)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                self.es_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")

