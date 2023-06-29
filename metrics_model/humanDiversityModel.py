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
                    codeDevQuality_decay
                    )

import logging
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)
from grimoire_elk.enriched.utils import get_time_diff_days
MAX_BULK_UPDATE_SIZE = 500

class HumanDiversityModel(MetricsModel):
    def __init__(self, issue_index=None, pr_index=None, repo_index=None, json_file=None, git_index=None, issue_comments_index=None, out_index=None, git_branch=None, from_date=None, end_date=None, community=None, level=None, company=None, pr_comments_index=None):
        super().__init__(json_file, from_date, end_date, out_index, community, level)
        self.git_index = git_index
        self.issue_index = issue_index
        self.repo_index = repo_index
        self.git_branch = git_branch
        self.pr_index = pr_index
        self.issue_comments_index = issue_comments_index
        self.pr_comments_index = pr_comments_index
        self.model_name = 'human Diversity'
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
                         ["tag"]}}for i in repos_list],
                         "minimum_should_match": 1,
                         "filter":
                         {"range":
        
                          {"grimoire_creation_date":
                           {"gte": from_date.strftime("%Y-%m-%d"), "lt": to_date.strftime("%Y-%m-%d")}}}
                      }
                     }]}},
        }
        return query
    
    def location_distribution(self, date, repos_list):
        query = self.get_org_count_query(repos_list, "tz", from_date=(date - timedelta(days=90)), to_date=date)
        tz_count = self.es_in.search(index=(self.git_index, self.issue_index, self.pr_index, self.issue_comments_index,self.pr_comments_index), body=query)['aggregations']["count_of_orgs"]['value']
        return tz_count

    def organization_count(self, date, repos_list):
        query = self.get_org_count_query(repos_list, "author_domain", from_date=(date - timedelta(days=90)), to_date=date)
        org_count = self.es_in.search(index=(self.git_index, self.issue_index, self.pr_index, self.issue_comments_index,self.pr_comments_index), body=query)[
            'aggregations']["count_of_orgs"]['value']
        return org_count

    def bus_factor(self, date, repos_list):
        query = {
            "size": 0,
            "track_total_hits": "true",
            "aggs": {
                "my_field_count": {
                    "terms": {
                        "field": "author_name",
                        "min_doc_count": 3
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
                         ["tag"]}}for i in repos_list],
                         "minimum_should_match": 1,
                         "filter":
                         {"range":
        
                          {"grimoire_creation_date":
                           {"gte": (date - timedelta(days=90)).strftime("%Y-%m-%d"), "lt": date.strftime("%Y-%m-%d")}}}
                      }
                     }]}},
        }
        answer = self.es_in.search(index=self.git_index, body=query)
        half_total = answer["hits"]["total"] / 2
        developers = answer["aggregations"]["my_field_count"]["buckets"]
        result = 0
        cur = 0
        for dev in developers:
            cur += dev["doc_count"]
            result += 1
            if cur >= half_total:
                break
        return result

    def elephant_factor(self, date, repos_list):
        query = {
            "size": 0,
            "track_total_hits": "true",
            "aggs": {
                "my_field_count": {
                    "terms": {
                        "field": "author_domain",
                        "min_doc_count": 3
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
                         ["tag"]}}for i in repos_list],
                         "minimum_should_match": 1,
                         "filter":
                         {"range":
        
                          {"grimoire_creation_date":
                           {"gte": (date - timedelta(days=90)).strftime("%Y-%m-%d"), "lt": date.strftime("%Y-%m-%d")}}}
                      }
                     }]}},
        }
        answer = self.es_in.search(index=self.git_index, body=query)
        half_total = answer["hits"]["total"] / 2
        developers = answer["aggregations"]["my_field_count"]["buckets"]
        result = 0
        cur = 0
        for dev in developers:
            cur += dev["doc_count"]
            result += 1
            if cur >= half_total:
                break
        return result 
               
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
            location_distribution = self.location_distribution(date, repos_list)
            print("location_distribution", location_distribution)
            organization_count = self.organization_count(date, repos_list)
            print("organization_count", organization_count)
            bus_factor = self.bus_factor(date, repos_list)
            print("bus_factor", bus_factor)
            elephant_factor = self.elephant_factor(date, repos_list)
            print("elephant_factor", elephant_factor)
            metrics_data = {
                'uuid': get_uuid(str(date), self.community, level, label, self.model_name, type),
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                'location_distribution': location_distribution,
                'organization_count': organization_count,
                'bus_factor': bus_factor,
                'elephant_factor': elephant_factor,
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
            score = get_humanDiversity_score(metrics_data, level)
            metrics_data["human_diversity_score"] = score
            print("human_diversity_score", score)
            item_datas.append(metrics_data)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                self.es_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")

