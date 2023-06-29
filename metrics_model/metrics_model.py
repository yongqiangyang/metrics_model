#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2021-2022 Yehui Wang, Chenqi Shan
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Yehui Wang <yehui.wang.mdh@gmail.com>
#     Chenqi Shan <chenqishan337@gmail.com>

from perceval.backend import uuid
from datetime import datetime, timedelta
from urllib.parse import urlparse
import json
import yaml
import pandas as pd
import logging
from grimoire_elk.enriched.utils import get_time_diff_days
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers
from elasticsearch.exceptions import NotFoundError
from grimoire_elk.elastic import ElasticSearch

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

import os
import inspect
import sys
current_dir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
os.chdir(current_dir)
sys.path.append('../')

logger = logging.getLogger(__name__)

MAX_BULK_UPDATE_SIZE = 500


class MetricsModel:
    def __init__(self, json_file, from_date, end_date, out_index=None, risk_index=None, community=None, level=None):
        """Metrics Model is designed for the integration of multiple CHAOSS metrics.
        :param json_file: the path of json file containing repository message.
        :param out_index: target index for Metrics Model.
        :param community: used to mark the repo belongs to which community.
        :param level: str representation of the metrics, choose from repo, project, community.
        """
        self.json_file = json_file
        self.out_index = out_index
        self.risk_index = risk_index
        self.community = community
        self.level = level
        self.from_date = from_date
        self.end_date = end_date
        self.date_list = get_date_list(from_date, end_date)

    def metrics_model_metrics(self, elastic_url):
        is_https = urlparse(elastic_url).scheme == 'https'
        self.es_in = Elasticsearch(
            elastic_url, use_ssl=is_https, verify_certs=False, connection_class=RequestsHttpConnection)
        self.es_out = ElasticSearch(elastic_url, self.out_index)
        print("here1")
        self.risk_out = ElasticSearch(elastic_url, self.risk_index)
        print("here2")
        print(self.level)
        all_repo_json = json.load(open(self.json_file))
        if self.level == "community":
            software_artifact_repos_list = []
            governance_repos_list = []
            for project in all_repo_json:
                origin = 'gitee' if 'gitee' in self.issue_index else 'github'
                origin_software_artifact = origin + "-software-artifact"
                origin_governance = origin + "-governance"
                for key in all_repo_json[project].keys():
                    if key == origin_software_artifact:
                        for j in all_repo_json[project].get(key):
                            software_artifact_repos_list.append(j)
                    if key == origin_governance:
                        for j in all_repo_json[project].get(key):
                            governance_repos_list.append(j)
            all_repo_list = software_artifact_repos_list + governance_repos_list
            if len(all_repo_list) > 0:
                for repo in all_repo_list:
                    # last_time = self.last_metrics_model_time(repo, self.model_name, "repo")
                    # if last_time is None:
                    #     self.metrics_model_enrich(repos_list=[repo], label=repo, level="repo",
                    #                               date_list=get_date_list(self.from_date, self.end_date))
                    # if last_time is not None and last_time < self.end_date:
                    #     self.metrics_model_enrich(repos_list=[repo], label=repo, level="repo",
                    #                               date_list=get_date_list(last_time, self.end_date))
                    self.metrics_model_enrich(repos_list=[repo], label=repo, level="repo",
                                                  date_list=get_date_list(self.from_date, self.end_date))
            if len(software_artifact_repos_list) > 0:
                self.metrics_model_enrich(software_artifact_repos_list, self.community, "software-artifact")
            if len(governance_repos_list) > 0:
                self.metrics_model_enrich(governance_repos_list, self.community, "governance")
        if self.level == "project":
            for project in all_repo_json:
                software_artifact_repos_list = []
                governance_repos_list = []
                origin = 'gitee' if 'gitee' in self.issue_index else 'github'
                origin_software_artifact = origin + "-software-artifact"
                origin_governance = origin + "-governance"
                for key in all_repo_json[project].keys():
                    if key == origin_software_artifact:
                        for j in all_repo_json[project].get(key):
                            software_artifact_repos_list.append(j)
                    if key == origin_governance:
                        for j in all_repo_json[project].get(key):
                            governance_repos_list.append(j)
                all_repo_list = software_artifact_repos_list + governance_repos_list
                if len(all_repo_list) > 0:
                    for repo in all_repo_list:
                        last_time = self.last_metrics_model_time(repo, self.model_name, "repo")
                        if last_time is None:
                            self.metrics_model_enrich(repos_list=[repo], label=repo, level="repo",
                                                      date_list=get_date_list(self.from_date, self.end_date))
                        if last_time is not None and last_time < self.end_date:
                            self.metrics_model_enrich(repos_list=[repo], label=repo, level="repo",
                                                      date_list=get_date_list(last_time, self.end_date))
                if len(software_artifact_repos_list) > 0:
                    self.metrics_model_enrich(software_artifact_repos_list, project, "software-artifact")
                if len(governance_repos_list) > 0:
                    self.metrics_model_enrich(governance_repos_list, project, "governance")
        if self.level == "repo":
            for project in all_repo_json:
                origin = 'gitee' if 'gitee' in self.issue_index else 'github'
                origin_software_artifact = origin + "-software-artifact"
                origin_governance = origin + "-governance"
                for key in all_repo_json[project].keys():
                    if key == origin_software_artifact or key == origin_governance or key == origin:
                        for j in all_repo_json[project].get(key):
                            self.metrics_model_enrich([j], j)

    def metrics_model_enrich(repos_list, label, type=None, level=None, date_list=None):
        pass

    def get_last_metrics_model_query(self, repo_url, model_name, level):
        query = {
                "size": 1,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match_phrase": {
                                    "label": repo_url
                            }
                            },
                            {
                                "term": {
                                    "model_name.keyword": model_name
                            }
                            },
                            {
                                "term": {
                                    "level.keyword":level
                            }
                            }
                        ]
                    }
                },
                "sort": [
                    {
                        "grimoire_creation_date": {
                            "order": "desc",
                            "unmapped_type": "keyword"
                        }
                    }
                ]
        }
        return query

    def last_metrics_model_time(self, repo_url, model_name, level):
        query = self.get_last_metrics_model_query(repo_url, model_name, level)
        try:
            query_hits = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"]
            return query_hits[0]["_source"]["grimoire_creation_date"] if query_hits.__len__() > 0 else None
        except NotFoundError:
            return None

    def created_since(self, date, repos_list):
        created_since_list = []
        for repo in repos_list:
            query_first_commit_since = self.get_updated_since_query(
                [repo], date_field='grimoire_creation_date', to_date=date, order="asc")
            first_commit_since = self.es_in.search(
                index=self.git_index, body=query_first_commit_since)['hits']['hits']
            if len(first_commit_since) > 0:
                creation_since = first_commit_since[0]['_source']["grimoire_creation_date"]
                created_since_list.append(
                    get_time_diff_months(creation_since, str(date)))
                # print(get_time_diff_months(creation_since, str(date)))
                # print(repo)
        if created_since_list:
            return sum(created_since_list)
        else:
            return None

    def get_uuid_count_query(self, option, repos_list, field, date_field="grimoire_creation_date", size=0, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query = {
            "size": size,
            "track_total_hits": "true",
            "aggs": {"count_of_uuid":
                     {option:
                      {"field": field}
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
                          {date_field:
                           {"gte": from_date.strftime("%Y-%m-%d"), "lt": to_date.strftime("%Y-%m-%d")}}}
                      }
                     }]}}
        }
        return query

    def get_uuid_count_contribute_query(self, repos_list, company=None, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query = {
            "size": 0,
            "aggs": {
                "count_of_contributors": {
                    "cardinality": {
                        "field": "author_name"
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

        if company:
            query["query"]["bool"]["must"] = [{"bool": {
                "should": [
                    {
                        "simple_query_string": {
                            "query": company + "*",
                            "fields": [
                                "author_domain"
                            ]
                        }
                    }],
                "minimum_should_match": 1}}]
        return query

    def get_updated_since_query(self, repos_list, date_field="grimoire_creation_date", order="desc", to_date=datetime_utcnow()):
        query = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match_phrase": {
                                "tag": repo
                            }} for repo in repos_list],
                    "minimum_should_match": 1,
                    "filter": {
                        "range": {
                            date_field: {
                                "lt": to_date.strftime("%Y-%m-%d")
                            }
                        }
                    }
                }
            },
            "sort": [
                {
                    date_field: {"order": order}
                }
            ]
        }
        return query

    def get_issue_closed_uuid_count(self, option, repos_list, field, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query = {
            "size": 0,
            "track_total_hits": True,
            "aggs": {
                "count_of_uuid": {
                    option: {
                        "field": field
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [{
                        "bool": {
                            "should": [{
                                "simple_query_string": {
                                    "query": i,
                                    "fields": ["tag"]
                                }}for i in repos_list],
                            "minimum_should_match": 1
                        }
                    }],
                    "must_not": [
                        {"term": {"state": "open"}},
                        {"term": {"state": "progressing"}}
                    ],
                    "filter": {
                        "range": {
                            "closed_at": {
                                "gte": from_date.strftime("%Y-%m-%d"),
                                "lt": to_date.strftime("%Y-%m-%d")
                            }
                        }
                    }
                }
            }
        }

        return query

    def get_pr_closed_uuid_count(self, option, repos_list, field, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query = {
            "size": 0,
            "track_total_hits": True,
            "aggs": {
                "count_of_uuid": {
                    option: {
                        "field": field
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [{
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
                    "must_not": [
                        {"term": {"state": "open"}},
                        {"term": {"state": "progressing"}}
                    ],
                    "filter": {
                        "range": {
                            "closed_at": {
                                "gte": from_date.strftime("%Y-%m-%d"),
                                "lt": to_date.strftime("%Y-%m-%d")
                            }
                        }
                    }
                }
            }
        }

        return query

    def get_recent_releases_uuid_count(self, option, repos_list, field, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query = {
            "size": 0,
            "track_total_hits": True,
            "aggs": {
                "count_of_uuid": {
                    option: {
                        "field": field + '.keyword'
                    }
                }
            },
            "query": {
                "bool": {
                    "must": [{
                        "bool": {
                            "should": [{
                                "simple_query_string": {
                                    "query": i,
                                    "fields": ["tag.keyword"]
                                }}for i in repos_list],
                            "minimum_should_match": 1
                        }
                    }
                    ],
                    "filter": {
                        "range": {
                            "grimoire_creation_date": {
                                "gte": from_date.strftime("%Y-%m-%d"),
                                "lt": to_date.strftime("%Y-%m-%d")
                            }
                        }
                    }
                }
            }
        }

        return query

    # name list of author_name in a index
    def get_all_CX_contributors(self, repos_list, search_index, pr=False, issue=False, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query_CX_users = {
            "aggs": {
                "name": {
                    "terms": {
                        "field": "author_name",
                        "size": 100000
                    }, "aggs": {
                        "date": {
                            "top_hits": {
                                "sort": [{
                                    "grimoire_creation_date": {"order": "asc"}
                                }],
                                "size": 1
                            }
                        }
                    }
                }
            },
            "query": {
                "bool": {
                    "should": [
                        {
                            "simple_query_string": {
                                "query": i+"(*) OR " + i+"*",
                                "fields": [
                                    "tag"
                                ]
                            }
                        } for i in repos_list
                    ],
                    "minimum_should_match": 1,
                    "filter": {
                        "range": {
                            "grimoire_creation_date": {
                                "gte": from_date.strftime("%Y-%m-%d"), "lte": to_date.strftime("%Y-%m-%d")
                            }
                        }
                    }
                }
            },
            "size": 0,
            "from": 0
        }
        if pr:
            query_CX_users["query"]["bool"]["must"] = {
                "match_phrase": {
                    "pull_request": "true"
                }
            }
        if issue:
            query_CX_users["query"]["bool"]["must"] = {
                "match_phrase": {
                    "pull_request": "false"
                }
            }
        CX_contributors = self.es_in.search(index=search_index, body=query_CX_users)[
            "aggregations"]["name"]["buckets"]
        return [i["date"]["hits"]["hits"][0]["_source"] for i in CX_contributors]

    def get_all_CX_comments_contributors(self, repos_list, search_index, pr=False, issue=False, from_date=str_to_datetime("1970-01-01"), to_date=datetime_utcnow()):
        query_CX_users = {
            "aggs": {
                "name": {
                    "terms": {
                        "field": "author_name",
                        "size": 100000
                    }, "aggs": {
                        "date": {
                            "top_hits": {
                                "sort": [{
                                    "grimoire_creation_date": {"order": "asc"}
                                }],
                                "size": 1
                            }
                        }
                    }
                }
            },
            "query": {
                "bool": {
                    "should": [
                        {
                            "simple_query_string": {
                                "query": i+"(*) OR " + i+"*",
                                "fields": [
                                    "tag"
                                ]
                            }
                        } for i in repos_list
                    ],
                    "minimum_should_match": 1,
                    "filter": {
                        "range": {
                            "grimoire_creation_date": {
                                "gte": from_date.strftime("%Y-%m-%d"), "lte": to_date.strftime("%Y-%m-%d")
                            }
                        }
                    }
                }
            },
            "size": 1,
            "from": 0
        }
        if pr:
            query_CX_users["query"]["bool"]["must"] = [
                {
                    "match_phrase": {
                        "item_type": "comment"
                    }
                }]
            # print(query_CX_users)
        if issue:
            query_CX_users["query"]["bool"]["must"] = [
                {
                    "match_phrase": {
                        "item_type": "comment"
                    }
                }, {
                    "match_phrase": {
                        "issue_pull_request": "false"
                    }
                }]
        CX_contributors = self.es_in.search(index=search_index, body=query_CX_users)[
            "aggregations"]["name"]["buckets"]
        all_contributors = [i["date"]["hits"]["hits"]
                            [0]["_source"] for i in CX_contributors]
        return all_contributors

    def query_commit_contributor_list(self, index, repo, from_date, to_date, page_size=100, search_after=[]):
        query = {
            "size": page_size,
            "query": {
                "bool": {
                "must": [
                    {
                    "match_phrase": {
                        "repo_name.keyword": repo
                    }    
                    }
                ],
                "filter": [
                    {
                    "range": {
                        "code_commit_date_list": {
                            "gte": from_date.strftime("%Y-%m-%d"),
                            "lte": to_date.strftime("%Y-%m-%d")
                        }
                    }
                    }
                ]
                }
            },
            "sort": [
                {
                    "_id": {
                        "order": "asc"
                    }
                }
            ]  
        }
        if len(search_after) > 0:
            query['search_after'] = search_after
        results = self.es_in.search(index=index, body=query)["hits"]["hits"]
        return results
    
    def get_commit_contributor_list(self, date, repos_list):
        result_list = []
        for repo in repos_list:
            search_after = []
            while True:
                contributor_list = self.query_commit_contributor_list(self.contributors_index, repo, date - timedelta(days=90), date, 500, search_after)
                if len(contributor_list) == 0:
                    break
                search_after = contributor_list[len(contributor_list) - 1]["sort"]
                result_list = result_list +[contributor["_source"] for contributor in contributor_list]
        return result_list   

    def org_count(self, date, contributor_list):
        org_name_set = set()
        from_date = (date - timedelta(days=90)).strftime("%Y-%m-%d")
        to_date = date.strftime("%Y-%m-%d")

        for contributor in contributor_list:
            for org in contributor["org_change_date_list"]:
                if  org.get("org_name") is not None and check_times_has_overlap(org["first_date"], org["last_date"], from_date, to_date):
                    org_name_set.add(org.get("org_name"))
        return len(org_name_set)

