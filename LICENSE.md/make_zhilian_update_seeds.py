#/bin/python
# -*- encoding:utf8 -*-
import sys
import time
import json
import psycopg2
from urllib import quote
from get_uid import get_uid

def pg_zp001():
    #conn = psycopg2.connect(host="172.20.205.107", port="5432", user="app_sn", password="app_sn_num2", database="postgres")
    conn = psycopg2.connect(host="172.20.205.109", port="3456", user="app_iwc", password="app_iwc_1",database="grabdb")
    #print conn
    cur = conn.cursor()

    table = 'zp001'
    company_name = '杭州阔知网络科技有限公司'
    # trend_table = 'MAC117'
    p = '''
        SELECT SEQ,ORGID_MGS_ZP001,ORGID_ZP001,ORGNAME_ZP001 FROM zp001 offset 13600 limit 100 
 
  '''

    # cur.execute(q.format(table=table))
    cur.execute(p)
    # _filter_map = {}
    #for i in cur.fetchall():
    #    print i
    #print cur.fetchall()[:10]
        # seq,  = i
        # _filter_map[seq] = True
    return cur.fetchall()

def make_zhilian():
    with open("recruit_zhilian_jsons_prod",'w') as fn:
        list_zhilian = pg_zp001()
        url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?jl=%E9%80%89%E6%8B%A9%E5%9C%B0%E5%8C%BA&pd=3&kw={company_name}&p=1&isadv=0'
        for _ in list_zhilian:
            url_web = url.format(company_name=_[3])
            seed_zhaopin = seed_list(url_web,_[3], orgid_mgs =_[1], orgid= _[2])
            fn.write('\n' + seed_zhaopin)       
    

def seed_list(url_web,company_name, orgid_mgs=0, orgid=0):

    cg_uid = get_uid(orgid_mgs + orgid)
    crawl_time = time.time()
    seed_zhaopin = {
        "raw_storage": [],
        "content_group": "IfindRecruit_" + cg_uid,
        "stat_tag":"True",
        "downloader": {
            "parser_queue": "lvpuxin_test_queue",
            "de": {
            },
            "header": {
                "Connection": "keep-alive",
                "H1ost": "sou.zhaopin.com",
                "Accept-Language": "zh-CN,zh;q=0.8",
                "Accept-Encoding": "gzip, deflate, sdch",
                "Cache-Control": "max-age=0",
                "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
            },
            "proxy": {
                "type": "normal"
            },
            "login": {
                "type": ""
            },
            "type": "get",
            "expand": {
                "OrderBy": "Update",
                "xpath": "//td[1]/div/a",
                "new_platform": "True",
                "xdepth": "1",
                "idepth": "1",
                "force_next_page": "True",
                "next_page_max": "100",
                "urlde_expire":"3",
                "unique_org" : company_name 
            }
        },
        "app": "iwencai",
        "parser": [
            {
                "parser_type": "xpath",
                "name": "zhilian",
                "detail": {
                    "company_name": {
                        "exec":"'%s'"%company_name
                    },
                    "orgid_mgs": {
                        "exec": "'%s'"%orgid_mgs
                    },
                    "orgid": {
                        "exec": "'%s'"%orgid
                    },
                    "f001v_zp002": {
                        "rule": "//div[@class='top-fixed-box']/div[1]/div[1]/h2/a/text()",
                        "exec": ""
                    },
                    "f001v_zp003": {
                        "type": "str",
                        "rule": "//div[@class='top-fixed-box']//div[@class='inner-left fl']/h1/text()",
                        "exec": ""
                    },
                    "ctime": {
                        "exec": "'now()'"
                    },
                    "f005n_zp003": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/ul/li[5]/strong/text()",
                        "exec": ""
                    },
                    "f003n_zp003": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/ul/li[1]/strong/text()",
                        "exec": ""
                    },
                    "f004v_temp": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/ul/li[2]/strong//text()",
                        "filter": "1:-1"
                    },
                   "f004v_zp003": {
                        "exec": "' '.join(f004v_temp)",
                        "sort": 101
                    },
                    "f002n_zp003": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/ul/li[1]/strong/text()",
                        "exec": ""
                    },
                    "f003d_zp002": {
                        "rule": "//span[@id='span4freshdate']/text()",
                        "exec": ""
                    },
                    "f009v_content": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/ul/li[8]/strong/a/text()",
                        "exec": ""
                    },
                    "f006n_zp003": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/ul/li[5]/strong/text()",
                        "exec": ""
                    },
                    "temp_content1": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/div[1]/div/div[1]/p/span/text()",
                        "filter": "1:-1"
                    },
                    "f010T_content1": {
                        "exec": "' '.join(temp_content1)",
                        "sort": 101
                    },
                    "temp_content2": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/div[1]/div/div[1]/p/text()",
                        "filter": "1:-1"
                    },
                    "f010T_content2": {
                        "exec": "' '.join(temp_content2)",
                        "sort": 101
                    },
                    "temp_content3": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/div[1]/div/div[1]//text()",
                        "filter": "1:-1"
                    },
                    "f010T_content3": {
                        "exec": "' '.join(temp_content3)",
                        "sort": 101
                    },
                    "temp_content4": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/div[1]/div/div[1]/div/p/span/span/text()",
                        "filter": "1:-1"
                    },
                    "f010T_content4": {
                        "exec": "' '.join(temp_content4)",
                        "sort": 101
                    },
                    "f010T_zp003": {
                        "exec": "' '.join(f010T_content)",
                        "sort": 101
                    },
                    "temp_content5": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/div[1]/div/div[1]/div[1]/text()",
                        "filter": "1:-1"
                    },
                    "f010t_content5": {
                        "exec": "' '.join(temp_content5)",
                        "sort": 101
                    },
                    "temp_content6": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/div[1]/div/div[1]/div/div/span/span/text()",
                    "filter": "1:-1"
                    },
                    "f010t_content6": {
                        "exec": "' '.join(temp_content6)",
                        "sort": 101
                    },
                    "f002d_zp002": {
                        "rule": "//span[@id='span4freshdate']/text()",
                        "exec": ""
                    },
                    "f007v_zp003": {
                        "type": "str",
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/ul/li[6]/strong/text()",
                        "exec": ""
                    },
                    "f008n_zp003": {
                        "rule": "//div[@class='terminalpage clearfix']/div[1]/ul/li[7]/strong/text()",
                        "exec": ""
                    }
                }
            }
        ],
        "priority": "0",
        "cluster": "weibo",
        "token": "ifind_recurit",
        "tag": "recurit_zhilian_%s"%get_uid(company_name),
        "beauty_storage": [
                #"mongodb://scrapyer:scrapy@192.168.207.187:27016/recruit?table=recruit_zhilian&unique_keys=orgid_mgs,url_web,orgid&op_keys=company_name,ctime,f001v_zp002,f001v_zp003,f002d_zp002,f002n_zp003,f003d_zp002,f003n_zp003,f004v_zp003,f005n_zp003,f006n_zp003,f007v_zp003,f008n_zp003,f009v_content,f010T_content1,f010T_content2,f010T_content3,f010T_content4,f010t_content5,f010t_content6,f010T_zp003,temp_content1,temp_content2,temp_content3,temp_content4,temp_content5,temp_content6,url_web,orgid_mgs,orgid&_DC_op_pairs=%7B%22url_web%22%3A%20%22_DC_result.url%22%7D"
                "mongodb://table=recruit_zhilian&unique_keys=orgid_mgs,url_web,orgid&op_keys=company_name,ctime,f001v_zp002,f001v_zp003,f002d_zp002,f002n_zp003,f003d_zp002,f003n_zp003,f004v_zp003,f005n_zp003,f006n_zp003,f007v_zp003,f008n_zp003,f009v_content,f010T_content1,f010T_content2,f010T_content3,f010T_content4,f010t_content5,f010t_content6,f010T_zp003,temp_content1,temp_content2,temp_content3,temp_content4,temp_content5,temp_content6,url_web,orgid_mgs,orgid&_DC_op_pairs=%7B%22url_web%22%3A%20%22_DC_result.url%22%7D"
        ],
        "crawl_time": crawl_time,
        "scheduler": {
            "refresh_rage": "0"
        },
        "entry": url_web,
        "type": "url",
        "method": "add",
        "notify": []
    }

    return json.dumps(seed_zhaopin)


make_zhilian()

