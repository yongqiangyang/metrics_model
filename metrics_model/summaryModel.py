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
                    get_summary_score,
                    codeDevQuality_decay
                    )

import logging
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
from grimoirelab_toolkit.datetime import (datetime_utcnow,
                                          str_to_datetime)
from grimoire_elk.enriched.utils import get_time_diff_days
MAX_BULK_UPDATE_SIZE = 500

class SummaryModel(MetricsModel):
    def __init__(self, issue_index=None, pr_index=None, repo_index=None, json_file=None, git_index=None, colic_index=None, git_aoc_index=None, out_index=None, risk_index=None, git_branch=None, from_date=None, end_date=None, community=None, level=None, company=None, pr_comments_index=None):
        super().__init__(json_file, from_date, end_date, out_index, risk_index, community, level)
        self.issue_index = issue_index
        self.git_index = git_index
        self.model_name = 'summary'
        self.company = None if company == None or company == 'None' else company

    def get_score_query(self, model_name, date, repos_list):
        query = {
                "query": {
                    "bool": {
                    "must": [
                        {"match_phrase": {"model_name": model_name}},
                        {"match": {"grimoire_creation_date": date}},
                        {
                        "bool": {
                            "should": [
                                [{
                                    "match_phrase": {
                                        "label": i
                                    }}for i in repos_list]
                            ]
                        }
                        }
                    ]
                    }
                },
                "sort": [
                    { "metadata__enriched_on": "asc" }
                ]
                }
        return query

    def code_develop_activity_score(self, date, repos_list):
        query = self.get_score_query("Code develop activity", date.isoformat(), repos_list)
        data = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]
        code_develop_activity_score = data["code_develop_activity_score"]
        is_maintained = data["is_maintained"]
        return code_develop_activity_score, is_maintained

    def code_develop_quality_score(self, date, repos_list):
        query = self.get_score_query("Code develop Quality", date.isoformat(), repos_list)
        data = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]
        code_develop_quality_score = data["code_develop_quality_score"]
        pr_first_response_time_avg = data["pr_first_response_time_avg"]
        pr_open_time_avg = data["pr_open_time_avg"]
        issue_first_reponse_avg = data["issue_first_reponse_avg"]
        issue_open_time_avg = data["issue_open_time_avg"]
        return code_develop_quality_score, pr_first_response_time_avg, pr_open_time_avg, issue_first_reponse_avg, issue_open_time_avg

    def code_security_score(self, date, repos_list):
        query = self.get_score_query("Code security", date.isoformat(), repos_list)
        data = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]
        code_security_score = data["code_security_score"]
        bug_issue_open_time_avg = data["bug_issue_open_time_avg"]
        vulnerability_count = data["vulnerability_count"]
        defect_count = data["defect_count"]
        code_smell = data["code_smell"]
        code_clone_percent = data["code_clone_percent"]
        code_cyclomatic_complexity = data["code_cyclomatic_complexity"]
        licenses_include_percent = data["licenses_include_percent"]
        return code_security_score, bug_issue_open_time_avg, vulnerability_count, defect_count, code_smell, code_clone_percent, code_cyclomatic_complexity, licenses_include_percent

    def community_activity_score(self, date, repos_list):
        query = self.get_score_query("community activity", date.isoformat(), repos_list)
        community_activity_score = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]["community_activity_score"]
        return community_activity_score
    
    def human_diversity_score(self, date, repos_list):
        query = self.get_score_query("human Diversity", date.isoformat(), repos_list)
        data = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]
        human_diversity_score = data["human_diversity_score"]
        bus_factor = data["bus_factor"]
        elephant_factor = data["elephant_factor"]
        return human_diversity_score, bus_factor, elephant_factor
    
    def tech_diversity_score(self, date, repos_list):
        query = self.get_score_query("technology Diversity", date.isoformat(), repos_list)
        data = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]
        tech_diversity_score = data["tech_diversity_score"]
        license_distribution = data["license_distribution"]
        return tech_diversity_score, license_distribution    

    def get_stat_query(self, model_name, date, repos_list):
        query = {
                "query": {
                    "bool": {
                    "must": [
                        {"match_phrase": {"model_name": model_name}},
                        {"match": {"grimoire_creation_date": date}}
                    ]
                    }
                },
                "sort": [
                    { "metadata__enriched_on": "asc" }
                ]
                }
        return query

    def CodeDevActivityStatistic(self, date, repos_list):
        query = self.get_stat_query("CodeDevActivityStatistic", date.isoformat(), repos_list)
        data = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]
        is_maintained_10percent = data["is_maintained_10percent"]
        is_maintained_30percent = data["is_maintained_30percent"]
        is_maintained_50percent = data["is_maintained_50percent"]
        return is_maintained_10percent, is_maintained_30percent, is_maintained_50percent

    def CodeDevQualityStatistic(self, date, repos_list):
        query = self.get_stat_query("CodeDevQualityStatistic", date.isoformat(), repos_list)
        data = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]
        pr_first_response_time_avg_10percent = data["pr_first_response_time_avg_10percent"]
        pr_first_response_time_avg_30percent = data["pr_first_response_time_avg_30percent"]
        pr_first_response_time_avg_50percent = data["pr_first_response_time_avg_50percent"]
        pr_open_time_avg_10percent = data["pr_open_time_avg_10percent"]
        pr_open_time_avg_30percent = data["pr_open_time_avg_30percent"]
        pr_open_time_avg_50percent = data["pr_open_time_avg_50percent"]
        issue_first_reponse_avg_10percent = data["issue_first_reponse_avg_10percent"]
        issue_first_reponse_avg_30percent = data["issue_first_reponse_avg_30percent"]
        issue_first_reponse_avg_50percent = data["issue_first_reponse_avg_50percent"]
        issue_open_time_avg_10percent = data["issue_open_time_avg_10percent"]
        issue_open_time_avg_30percent = data["issue_open_time_avg_30percent"]
        issue_open_time_avg_50percent = data["issue_open_time_avg_50percent"]
        return pr_first_response_time_avg_10percent, pr_first_response_time_avg_30percent, pr_first_response_time_avg_50percent, pr_open_time_avg_10percent, pr_open_time_avg_30percent, pr_open_time_avg_50percent, issue_first_reponse_avg_10percent,issue_first_reponse_avg_30percent, issue_first_reponse_avg_50percent, issue_open_time_avg_10percent, issue_open_time_avg_30percent, issue_open_time_avg_50percent

    def CodeSecurityStatistic(self, date, repos_list):
        query = self.get_stat_query("CodeSecurityStatistic", date.isoformat(), repos_list)
        data = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]
        bug_issue_open_time_avg_10percent = data["bug_issue_open_time_avg_10percent"]
        bug_issue_open_time_avg_30percent = data["bug_issue_open_time_avg_30percent"]
        bug_issue_open_time_avg_50percent = data["bug_issue_open_time_avg_50percent"]
        vulnerability_count_10percent = data["vulnerability_count_10percent"]
        vulnerability_count_30percent = data["vulnerability_count_30percent"]
        vulnerability_count_50percent = data["vulnerability_count_50percent"]
        defect_count_10percent = data["defect_count_10percent"]
        defect_count_30percent = data["defect_count_30percent"]
        defect_count_50percent = data["defect_count_50percent"]
        code_smell_10percent = data["code_smell_10percent"]
        code_smell_30percent = data["code_smell_30percent"]
        code_smell_50percent = data["code_smell_50percent"]
        code_clone_percent_10percent = data["code_clone_percent_10percent"]
        code_clone_percent_30percent = data["code_clone_percent_30percent"]
        code_clone_percent_50percent = data["code_clone_percent_50percent"]
        code_cyclomatic_complexity_10percent = data["code_cyclomatic_complexity_10percent"]
        code_cyclomatic_complexity_30percent = data["code_cyclomatic_complexity_30percent"]
        code_cyclomatic_complexity_50percent = data["code_cyclomatic_complexity_50percent"]   
        licenses_include_percent_10percent = data["licenses_include_percent_10percent"]
        licenses_include_percent_30percent = data["licenses_include_percent_30percent"]
        licenses_include_percent_50percent = data["licenses_include_percent_50percent"]   
        return bug_issue_open_time_avg_10percent, bug_issue_open_time_avg_30percent, bug_issue_open_time_avg_50percent, vulnerability_count_10percent, vulnerability_count_30percent, vulnerability_count_50percent, defect_count_10percent, defect_count_30percent, defect_count_50percent, code_smell_10percent, code_smell_30percent, code_smell_50percent, code_clone_percent_10percent, code_clone_percent_30percent, code_clone_percent_50percent, code_cyclomatic_complexity_10percent, code_cyclomatic_complexity_30percent, code_cyclomatic_complexity_50percent, licenses_include_percent_10percent, licenses_include_percent_30percent, licenses_include_percent_50percent

    def HumanDiversityStatistic(self, date, repos_list):
        query = self.get_stat_query("HumanDiversityStatistic", date.isoformat(), repos_list)
        data = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]
        bus_factor_10percent = data["bus_factor_10percent"]
        bus_factor_30percent = data["bus_factor_30percent"]
        bus_factor_50percent = data["bus_factor_50percent"]
        elephant_factor_10percent = data["elephant_factor_10percent"]
        elephant_factor_30percent = data["elephant_factor_30percent"]
        elephant_factor_50percent = data["elephant_factor_50percent"]
        return bus_factor_10percent, bus_factor_30percent, bus_factor_50percent, elephant_factor_10percent, elephant_factor_30percent, elephant_factor_50percent

    def TechDiversityStatistic(self, date, repos_list):
        query = self.get_stat_query("TechDiversityStatistic", date.isoformat(), repos_list)
        data = self.es_in.search(index=self.out_index, body=query)["hits"]["hits"][-1]["_source"]
        license_distribution_10percent = data["license_distribution_10percent"]
        license_distribution_30percent = data["license_distribution_30percent"]
        license_distribution_50percent = data["license_distribution_50percent"]
        return license_distribution_10percent, license_distribution_30percent, license_distribution_50percent

    def get_risk_level(slef, data, data_10percent, data_30percent, data_50percent):
        if data is None or data_10percent is None or data_30percent is None or data_50percent is None:
            return "良好"
        if data < data_10percent:
            return "严重预警"   
        elif data < data_30percent:
            return "中度预警"
        elif data < data_50percent:
            return "轻度预警"
        else:
            return "良好"

    def metrics_model_enrich(self, repos_list, label, type=None, level=None, date_list=None):
        print("here")
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
            code_develop_activity_score, is_maintained = self.code_develop_activity_score(date, repos_list)
            code_develop_quality_score, pr_first_response_time_avg, pr_open_time_avg, issue_first_reponse_avg, issue_open_time_avg = self.code_develop_quality_score(date, repos_list)
            code_security_score, bug_issue_open_time_avg, vulnerability_count, defect_count, code_smell, code_clone_percent, code_cyclomatic_complexity, licenses_include_percent = self.code_security_score(date, repos_list)
            community_activity_score = self.community_activity_score(date, repos_list)
            human_diversity_score, bus_factor, elephant_factor = self.human_diversity_score(date, repos_list)
            tech_diversity_score, license_distribution = self.tech_diversity_score(date, repos_list)
            is_maintained_10percent, is_maintained_30percent, is_maintained_50percent = self.CodeDevActivityStatistic(date, repos_list)
            pr_first_response_time_avg_10percent, pr_first_response_time_avg_30percent, pr_first_response_time_avg_50percent, pr_open_time_avg_10percent, pr_open_time_avg_30percent, pr_open_time_avg_50percent, issue_first_reponse_avg_10percent,issue_first_reponse_avg_30percent, issue_first_reponse_avg_50percent, issue_open_time_avg_10percent, issue_open_time_avg_30percent, issue_open_time_avg_50percent = self.CodeDevQualityStatistic(date, repos_list)
            bug_issue_open_time_avg_10percent, bug_issue_open_time_avg_30percent, bug_issue_open_time_avg_50percent, vulnerability_count_10percent, vulnerability_count_30percent, vulnerability_count_50percent, defect_count_10percent, defect_count_30percent, defect_count_50percent, code_smell_10percent, code_smell_30percent, code_smell_50percent, code_clone_percent_10percent, code_clone_percent_30percent, code_clone_percent_50percent, code_cyclomatic_complexity_10percent, code_cyclomatic_complexity_30percent, code_cyclomatic_complexity_50percent, licenses_include_percent_10percent, licenses_include_percent_30percent, licenses_include_percent_50percent = self.CodeSecurityStatistic(date, repos_list)
            bus_factor_10percent, bus_factor_30percent, bus_factor_50percent, elephant_factor_10percent, elephant_factor_30percent, elephant_factor_50percent = self.HumanDiversityStatistic(date, repos_list)
            license_distribution_10percent, license_distribution_30percent, license_distribution_50percent = self.TechDiversityStatistic(date, repos_list)
            metrics_data = {
                'uuid': get_uuid(str(date), self.community, level, label, self.model_name, type),
                'level': level,
                'type': type,
                'label': label,
                'model_name': self.model_name,
                'copy_code_develop_activity_score': code_develop_activity_score,
                'copy_code_develop_quality_score': code_develop_quality_score,
                'copy_code_security_score': code_security_score,
                'copy_community_activity_score': community_activity_score,
                'copy_human_diversity_score': human_diversity_score,
                'copy_tech_diversity_score' : tech_diversity_score,
                'grimoire_creation_date': date.isoformat(),
                'metadata__enriched_on': datetime_utcnow().isoformat()
            }
            score = get_summary_score(metrics_data, level)

            metrics_data["summary_score"] = score
            
            metrics_data["is_maintained_risk"] = self.get_risk_level(is_maintained, is_maintained_10percent, is_maintained_30percent,is_maintained_50percent)
            metrics_data["pr_first_response_time_avg_risk"] = self.get_risk_level(pr_first_response_time_avg, pr_first_response_time_avg_10percent, pr_first_response_time_avg_30percent,pr_first_response_time_avg_50percent)
            metrics_data["pr_open_time_avg_risk"] = self.get_risk_level(pr_open_time_avg, pr_open_time_avg_10percent, pr_open_time_avg_30percent,pr_open_time_avg_50percent)
            metrics_data["issue_first_reponse_avg_risk"] = self.get_risk_level(issue_first_reponse_avg, issue_first_reponse_avg_10percent, issue_first_reponse_avg_30percent, issue_first_reponse_avg_50percent)
            metrics_data["issue_open_time_avg_risk"] = self.get_risk_level(issue_open_time_avg, issue_open_time_avg_10percent, issue_open_time_avg_30percent, issue_open_time_avg_50percent)
            # bug_issue_open_time_avg, vulnerability_count, defect_count, code_smell, code_clone_percent, code_cyclomatic_complexity, licenses_include_percent
            metrics_data["bug_issue_open_time_avg_risk"] = self.get_risk_level(bug_issue_open_time_avg, bug_issue_open_time_avg_10percent, bug_issue_open_time_avg_30percent, bug_issue_open_time_avg_50percent)
            metrics_data["vulnerability_count_risk"] = self.get_risk_level(vulnerability_count, vulnerability_count_10percent, vulnerability_count_30percent, vulnerability_count_50percent)
            metrics_data["defect_count_risk"] = self.get_risk_level(defect_count, defect_count_10percent, defect_count_30percent, defect_count_50percent)
            metrics_data["code_smell_risk"] = self.get_risk_level(code_smell, code_smell_10percent, code_smell_30percent, code_smell_50percent)
            metrics_data["code_clone_percent_risk"] = self.get_risk_level(code_clone_percent, code_clone_percent_10percent, code_clone_percent_30percent, code_clone_percent_50percent)
            metrics_data["code_cyclomatic_complexity_risk"] = self.get_risk_level(code_cyclomatic_complexity, code_cyclomatic_complexity_10percent, code_cyclomatic_complexity_30percent, code_cyclomatic_complexity_50percent)
            metrics_data["licenses_include_percent_risk"] = self.get_risk_level(licenses_include_percent, licenses_include_percent_10percent, licenses_include_percent_30percent, licenses_include_percent_50percent)
            # bus_factor, elephant_factor
            metrics_data["bus_factor_risk"] = self.get_risk_level(bus_factor, bus_factor_10percent, bus_factor_30percent, bus_factor_50percent)
            metrics_data["elephant_factor_risk"] = self.get_risk_level(elephant_factor, elephant_factor_10percent, elephant_factor_30percent, elephant_factor_50percent)
            # license_distribution
            metrics_data["license_distribution_risk"] = self.get_risk_level(license_distribution, license_distribution_10percent, license_distribution_30percent, license_distribution_50percent)

            item_datas.append(metrics_data)
            # print(metrics_data)
            if len(item_datas) > MAX_BULK_UPDATE_SIZE:
                self.es_out.bulk_upload(item_datas, "uuid")
                self.risk_out.bulk_upload(item_datas, "uuid")
                item_datas = []
        self.es_out.bulk_upload(item_datas, "uuid")
        self.risk_out.bulk_upload(item_datas, "uuid")

