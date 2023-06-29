from metrics_model.codeDevActivityModel import CodeDevActivityModel
from metrics_model.codeDevQualityModel import CodeDevQualityModel
from metrics_model.codeSecurityModel import CodeSecurityModel
from metrics_model.communityActivityModel import CommunityActivityModel
from metrics_model.humanDiversityModel import HumanDiversityModel
from metrics_model.techDiversityModel import TechDiversityModel
from metrics_model.summaryModel import SummaryModel
from metrics_model.statModel import (
   CodeDevActivityStatistic,
   CodeDevQualityStatistic,
   CodeSecurityStatistic,
   CommunityActivityStatistic,
   HumanDiversityStatistic,
   TechDiversityStatistic
)

import yaml

def process(cofig_url, elastic_url, params):
    # 代码开发活动
    kwargs = {}
    for item in ['git_index', 'issue_index', 'repo_index', 'pr_index', 'pr_comments_index', 'json_file', 'out_index', 'git_branch', 'from_date', 'end_date', 'community', 'level', 'company']:
        kwargs[item] = params[item]
    model_CodeDevActivity = CodeDevActivityModel(**kwargs)
    model_CodeDevActivity.metrics_model_metrics(elastic_url)

    # 代码开发效率
    kwargs = {}
    for item in ['git_index', 'issue_index', 'repo_index', 'pr_index', 'pr_comments_index', 'json_file', 'out_index', 'git_branch', 'from_date', 'end_date', 'community', 'level', 'company']:
        kwargs[item] = params[item]
    model_CodeDevQuality = CodeDevQualityModel(**kwargs)
    model_CodeDevQuality.metrics_model_metrics(elastic_url)

    # 代码安全保障
    kwargs = {}
    for item in ['git_index', 'issue_index', 'repo_index', 'pr_index', 'pr_comments_index', 'cocom_index', 'colic_index', 'json_file', 'out_index', 'git_branch', 'from_date', 'end_date', 'community', 'level', 'company']:
        kwargs[item] = params[item]
    model_CodeSecurity = CodeSecurityModel(**kwargs)
    model_CodeSecurity.metrics_model_metrics(elastic_url)   

    # 社区活跃度
    kwargs = {}
    for item in ['git_index', 'issue_index', 'repo_index', 'pr_index', 'issue_comments_index', 'pr_comments_index', 'json_file', 'out_index', 'git_branch', 'from_date', 'end_date', 'community', 'level', 'company']:
        kwargs[item] = params[item]
    model_communityActivity = CommunityActivityModel(**kwargs)
    model_communityActivity.metrics_model_metrics(elastic_url)

    # 人员多样性
    kwargs = {}
    for item in ['git_index', 'issue_index', 'repo_index', 'pr_index', 'issue_comments_index', 'pr_comments_index', 'json_file', 'out_index', 'git_branch', 'from_date', 'end_date', 'community', 'level', 'company']:
        kwargs[item] = params[item]
    model_humanDiversity = HumanDiversityModel(**kwargs)
    model_humanDiversity.metrics_model_metrics(elastic_url)

    # 技术多样性
    kwargs = {}
    for item in ['git_index', 'issue_index', 'repo_index', 'pr_index', 'pr_comments_index', 'git_aoc_index', 'colic_index','json_file', 'out_index', 'git_branch', 'from_date', 'end_date', 'community', 'level', 'company']:
        kwargs[item] = params[item]
    model_techDiversity = TechDiversityModel(**kwargs)
    model_techDiversity.metrics_model_metrics(elastic_url)

    # 总结
    kwargs = {}
    for item in ['issue_index', 'git_index', 'json_file', 'out_index', 'risk_index', 'git_branch', 'from_date', 'end_date', 'community', 'level', 'company']:
        kwargs[item] = params[item]
    model_summary = SummaryModel(**kwargs)
    model_summary.metrics_model_metrics(elastic_url)    

if __name__ == '__main__':
    cofig_urls = ["/home/ruoxuan/metrics-model/community-gitee-openeuler.yaml", "/home/ruoxuan/metrics-model/community-gitee-openharmony.yaml", "/home/ruoxuan/metrics-model/project-gitee.yaml", "/home/ruoxuan/metrics-model/project-github.yaml"]
    # cofig_urls = ["/home/ruoxuan/metrics-model/project-github.yaml"]
    
    for cofig_url in cofig_urls:
        CONF = yaml.safe_load(open(cofig_url))
        # print(CONF)
        elastic_url = CONF['url']
        params = CONF['params']
        process(cofig_url, elastic_url, params)


    # 代码开发活动
    stat_CodeDevActivity = CodeDevActivityStatistic(params['out_index'], 'Code develop activity', params['from_date'], params['end_date'], params['out_index'])
    stat_CodeDevActivity.metrics_model_statistic(elastic_url)

    # 代码开发效率
    stat_CodeDevQuality = CodeDevQualityStatistic(params['out_index'], 'Code develop Quality', params['from_date'], params['end_date'], params['out_index'])
    stat_CodeDevQuality.metrics_model_statistic(elastic_url)

    # 代码开发质量
    stat_CodeSecurity = CodeSecurityStatistic(params['out_index'], 'Code security', params['from_date'], params['end_date'], params['out_index'])
    stat_CodeSecurity.metrics_model_statistic(elastic_url)

    # 社区活跃度
    stat_CommunityActivity = CommunityActivityStatistic(params['out_index'], 'community activity', params['from_date'], params['end_date'], params['out_index'])
    stat_CommunityActivity.metrics_model_statistic(elastic_url)    

    # 人员多样性
    stat_HumanDiversity = HumanDiversityStatistic(params['out_index'], 'human Diversity', params['from_date'], params['end_date'], params['out_index'])
    stat_HumanDiversity.metrics_model_statistic(elastic_url)    

    # 技术多样性
    stat_TechDiversity = TechDiversityStatistic(params['out_index'], 'technology Diversity', params['from_date'], params['end_date'], params['out_index'])
    stat_TechDiversity.metrics_model_statistic(elastic_url)  