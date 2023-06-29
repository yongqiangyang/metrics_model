
import logging

from functools import reduce
from urllib.parse import urlparse

from perceval.backend import uuid

from grimoire_elk.elastic import ElasticSearch
from grimoirelab_toolkit.datetime import datetime_utcnow

from elasticsearch import Elasticsearch, RequestsHttpConnection

from .utils import (get_uuid, get_date_list)

MAX_BULK_UPDATE_SIZE = 5000

logger = logging.getLogger(__name__)

class statModel:
    """
    MetricsSummary mainly designed to summarize the global data of MetricsModel.
    :param metric_index: summarization target index name
    :param from_date: summarization start date
    :param end_date: summarization end date
    :param out_index: summarization storage index name
    """
    def __init__(self, metric_index, model_name, from_date, end_date, out_index):
        self.from_date = from_date
        self.end_date = end_date
        self.out_index = out_index
        self.model_name = model_name
        self.metric_index = metric_index
        self.summary_name = self.__class__.__name__

    def base_stat_method(self, field):
        query = {
            f"{field}_mean": {
                'avg': {
                    'field': field
                }
            },
            f"{field}_percent": {
                'percentiles': {
                    'field': field,
                    'percents': [10, 30, 50]
                }
            },
            f"{field}_min": {
                'min': {
                    'field': field
                }
            },
            f"{field}_max": {
                'max': {
                    'field': field
                }
            },
        }
        return query

    def apply_stat_method(self, fields):
        return reduce(lambda query, field: {**self.base_stat_method(field), **query}, fields, {})

    def metrics_model_summary_query(self, date=None):
        query = {
            "size": 0,
            "from": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "grimoire_creation_date": {
                                    "lte": date.strftime("%Y-%m-%d"),
                                    "gte": date.strftime("%Y-%m-%d")
                                }
                            }
                        },
                        {
                            "term": {
                                "model_name.keyword": self.model_name
                            }
                        }
                    ]
                }
            },
            "aggs": self.apply_stat_method(self.summary_fields())
        }
        body = self.es_in.search(index=self.metric_index, body=query)
        return body

    def metrics_model_enrich(self, result, field):
        result['res'] = {
            **result['res'],
            **{
                f"{field}_mean": result['aggs'][f"{field}_mean"]['value'],
                f"{field}_10percent": result['aggs'][f"{field}_percent"]['values']['10.0'],
                f"{field}_30percent": result['aggs'][f"{field}_percent"]['values']['30.0'],
                f"{field}_50percent": result['aggs'][f"{field}_percent"]['values']['50.0'],
                f"{field}_max": result['aggs'][f"{field}_max"]['value'],
                f"{field}_min": result['aggs'][f"{field}_min"]['value'],
            }
        }
        return result

    def metrics_model_after_query(self, response):
        aggregations = response.get('aggregations')
        return reduce(self.metrics_model_enrich, self.summary_fields(), {'aggs': aggregations, 'res': {}})

    def metrics_model_statistic(self, elastic_url):
        is_https = urlparse(elastic_url).scheme == 'https'
        self.es_in = Elasticsearch(
            elastic_url, use_ssl=is_https, verify_certs=False, connection_class=RequestsHttpConnection)
        self.es_out = ElasticSearch(elastic_url, self.out_index)
        date_list = get_date_list(self.from_date, self.end_date)

        item_datas = []
        for date in date_list:
            print(str(date) + "--" + self.summary_name)
            response = self.metrics_model_summary_query(date)
            summary_data = self.metrics_model_after_query(response)['res']
            summary_meta = {
                'uuid': get_uuid(str(date), self.summary_name),
                'model_name': self.summary_name,
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
            summary_item = {**summary_meta, **summary_data}
            item_datas.append(summary_item)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                self.es_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")

class CodeDevActivityStatistic(statModel):
    def summary_fields(self):
        return [
            'LOC_frequency',
            'contributor_count',
            'pr_count',
            'commit_frequency',
            'updated_issues_count',
            'is_maintained',
        ]

class CodeDevQualityStatistic(statModel):
    def summary_fields(self):
        return [
            'code_review_ratio',
            'code_merge_ratio',
            'git_pr_linked_ratio',
            'pr_issue_linked_ratio',
            'pr_first_response_time_avg',
            'pr_first_response_time_mid',
            'pr_open_time_avg',
            'pr_open_time_mid',
            'issue_first_reponse_avg',
            'issue_first_reponse_mid',
            'issue_open_time_avg',
            'issue_open_time_mid',
            'comment_frequency',
        ]

class CodeSecurityStatistic(statModel):
    def summary_fields(self):
        return [
            'bug_issue_open_time_avg',
            'bug_issue_open_time_mid',
            'vulnerability_count',
            'defect_count',
            'code_smell',
            'code_clone_percent',
            'code_cyclomatic_complexity',
            'licenses_include_percent'
        ]

class CommunityActivityStatistic(statModel):
    def summary_fields(self):
        return [
            'contributors_number',
            'updated_at',
            'created_at'
        ]
class HumanDiversityStatistic(statModel):
    def summary_fields(self):
        return [
            'location_distribution',
            'organization_count',
            'bus_factor',
            'elephant_factor'
        ]
class TechDiversityStatistic(statModel):
    def summary_fields(self):
        return [
            'language_distribution',
            'license_distribution',
        ]