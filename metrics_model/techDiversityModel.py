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
                    get_humanDiversity_score,
                    get_techDiversity_score,
                    codeDevQuality_decay
                    )

import logging
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)
from grimoire_elk.enriched.utils import get_time_diff_days
MAX_BULK_UPDATE_SIZE = 500

class TechDiversityModel(MetricsModel):
    def __init__(self, issue_index=None, pr_index=None, repo_index=None, json_file=None, git_index=None, colic_index=None, git_aoc_index=None, out_index=None, git_branch=None, from_date=None, end_date=None, community=None, level=None, company=None, pr_comments_index=None):
        super().__init__(json_file, from_date, end_date, out_index, community, level)
        self.git_index = git_index
        self.issue_index = issue_index
        self.repo_index = repo_index
        self.git_branch = git_branch
        self.pr_index = pr_index
        self.pr_comments_index = pr_comments_index
        self.git_aoc_index = git_aoc_index
        self.colic_index = colic_index
        self.model_name = 'technology Diversity'
        self.company = None if company == None or company == 'None' else company
    
    def get_org_count_query(self, repos_list, field, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query = {
            "size": 0,
            "aggs": {
                "count_of_orgs": {
                    "cardinality": {
                        "field": field
                    }
                }
            },
            "query":
            {"bool": {
                "must": [
                    {"bool":
                     {"should":
                      [{"simple_query_string":
                        {"query": i + "*",
                         "fields":
                         ["origin"]}}for i in repos_list],
                         "minimum_should_match": 1,
                         "filter":
                         {"range":
        
                          {"grimoire_creation_date":
                           {"gte": from_date.strftime("%Y-%m-%d"), "lt": to_date.strftime("%Y-%m-%d")}}}
                      }
                     }]}},
        }
        return query

    def language_distribution(self, date, repos_list):
        query = self.get_org_count_query(repos_list, "file_ext", from_date=(date - timedelta(days=90)), to_date=date)
        # print("query", query)
        language_count = self.es_in.search(index=self.git_aoc_index, body=query)[
            'aggregations']["count_of_orgs"]['value']
        return language_count

    def license_distribution(self, date, repos_list):
        query = self.get_org_count_query(repos_list, "license_name", from_date=(date - timedelta(days=90)), to_date=date)
        # print("query", query)
        language_count = self.es_in.search(index=self.colic_index, body=query)[
            'aggregations']["count_of_orgs"]['value']
        return language_count


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
            language_distribution = self.language_distribution(date, repos_list)
            # print("language_distribution",language_distribution)
            license_distribution = self.license_distribution(date, repos_list)
            # print("license_distribution", license_distribution)
            metrics_data = {
                'uuid': get_uuid(str(date), self.community, level, label, self.model_name, type),
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                'language_distribution': language_distribution,
                'license_distribution': license_distribution,
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
            score = get_techDiversity_score(metrics_data, level)
            metrics_data["tech_diversity_score"] = score
            print("tech_diversity_score", score)
            item_datas.append(metrics_data)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                self.es_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")

