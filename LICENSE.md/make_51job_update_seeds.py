#/bin/python
# -*- encoding:utf8 -*-
import sys
import time
import json
import psycopg2
import requests
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
        SELECT SEQ,ORGID_MGS_ZP001,ORGID_ZP001,ORGNAME_ZP001 FROM zp001  offset 14500 limit 100 
 
  '''

    cur.execute(p)
    # _filter_map = {}
    #for i in cur.fetchall():
    #    print i
    #print cur.fetchall()[:10]
        # seq,  = i
        # _filter_map[seq] = True
    return cur.fetchall()

def make_51job():
    with open("recruit_51job_jsons_prod",'w') as fn:
        list_51job = pg_zp001()
        url = 'http://search.51job.com/list/000000,000000,0000,00,1,99,{company_name},1,1.html?keywordtype=1'
        for _ in list_51job:
            #if _[3].find('伊利') == -1:
            #    continue
            #if _[3].find('内蒙古伊利实业集团股份有限公司') == -1:
            #    continue
            url_web = url.format(company_name=_[3])
            print url_web
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
        "Host": "jobs.51job.com",
        "Accept-Language": "zh-CN,zh;q=0.8",
        "Accept-Encoding": "gzip, deflate, sdch",
        "Cache-Control": "max-age=0"
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
        "xpath": "//*[@id=\"resultList\"]/div/p/span/a",
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
        "name": "51job",
        "detail": {
         "company_name": {
          "exec": "'%s'"%company_name
         },
         "orgid_mgs": {
          "exec": "'%s'"%orgid_mgs
         },
         "orgid": {
          "exec": "'%s'"%orgid
         },
         "f009v_content1": {
          "rule": "//div[@class='tCompany_main']/div[2]/div/div[1]/p/span[2]/text()",
          "filter": "1"
         },
         "f001v_zp002": {
          "rule": "//div[@class='tHeader tHjob']//div[@class='cn']/p[1]/a/text()",
          "exec": ""
         },
         "f001v_zp003": {
          "type": "str",
          "rule": "//div[@class='tHeader tHjob']//div[@class='cn']/h1/text()",
          "exec": ""
         },
         "ctime": {
          "exec": "'now()'"
         },
         "f005n_zp003": {
          "rule": "//div[@class='tCompany_main']//em[@class='i1']/parent::node()/text()",
          "exec": ""
         },
         "f003n_zp003": {
          "rule": "//div[@class='tHeader tHjob']//div[@class='cn']/strong/text()",
          "exec": ""
         },
         "f004v_zp003": {
          "type": "str",
          "rule": "//div[@class='tHeader tHjob']//div[@class='cn']/span/text()",
          "exec": ""
         },
         "f002n_zp003": {
          "rule": "//div[@class='tHeader tHjob']//div[@class='cn']/strong/text()",
          "exec": ""
         },
         "f003d_zp002": {
          "rule": "//div[@class='tCompany_main']//em[@class='i4']/parent::node()/text()",
          "exec": ""
         },
         "f009v_content": {
          "rule": "//div[@class='tCompany_main']/div[4]/div/div/p[1]/span/text()",
          "filter": "1"
         },
         "f009v_content1": {
          "rule": "//div[@class='tCompany_main']/div[2]/div/div[1]/p/span[2]/text()",
          "filter": "1"
         },
         "f006n_zp003": {
          "rule": "//div[@class='tCompany_main']//em[@class='i1']/parent::node()/text()",
          "exec": ""
         },
         "temp_content1": {
          "rule": "//div[@class='tCompany_main']//span[@class='label']/parent::node()//text()",
          "filter": "2:-1"
         },
         "f010T_content1": {
          "exec": "' '.join(temp_content1)",
          "sort": 101
         },
         "temp_content2": {
          "rule": "//div[@class='tCompany_main']/div[4]/div/span/p/span/text()",
          "filter": "2:-1"
         },
         "f010T_content2": {
          "exec": "' '.join(temp_content2)",
          "sort": 101
         },
         "f002d_zp002": {
          "rule": "//div[@class='tCompany_main']//em[@class='i4']/parent::node()/text()",
          "exec": ""
         },
         "f007v_zp003": {
          "type": "str",
          "rule": "//div[@class='tCompany_main']//em[@class='i2']/parent::node()/text()",
          "exec": ""
         },
         "f008n_zp003": {
          "rule": "//div[@class='tCompany_main']//em[@class='i3']/parent::node()/text()",
          "exec": ""
         }
        }
       }
      ],
      "priority": "0",
      "cluster": "common",
      "token": "ifind_recurit",
      "tag": "recurit_51job_%s"%get_uid(company_name),
      "beauty_storage": [
       #"mongodb://?table=recruit_test_51job&unique_keys=f001v_zp003,url_web&op_keys=company_name,ctime,f001v_zp002,f001v_zp003,f002d_zp002,f002n_zp003,f003d_zp002,f003n_zp003,f004v_zp003,f005n_zp003,f006n_zp003,f007v_zp003,f008n_zp003,f009v_content,f010T_content1,f010T_content2,temp_content1,temp_content2,url_web&_DC_op_pairs=%7B%22url_web%22%3A%20%22_DC_result.url%22%7D"
       "mongodb://?table=recruit_51job&unique_keys=orgid_mgs,url_web,orgid&op_keys=company_name,ctime,f001v_zp002,f001v_zp003,f002d_zp002,f002n_zp003,f003d_zp002,f003n_zp003,f004v_zp003,f005n_zp003,f006n_zp003,f007v_zp003,f008n_zp003,f009v_content,f010T_content1,f010T_content2,temp_content1,temp_content2,url_web,orgid_mgs,orgid,f009v_content1&_DC_op_pairs=%7B%22url_web%22%3A%20%22_DC_result.url%22%7D"
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


make_51job()

