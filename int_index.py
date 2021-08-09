import pandas as pd
import numpy as np
import argparse
import scipy.sparse as sps
import os
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',level=logging.INFO)
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
import psycopg2 as pg
import pickle
import gzip
from datetime import datetime


def data_load(filename, cursor):
    product = cursor
    f = open(filename, 'r')
    
    text = ''
    while True:
        line = f.readline()        
        if not line: break
        a = str(line)
        text = text + a
    f.close()
    
    data = pd.read_sql(text, product) 
    print("#------Read SQL Completed!------#")
    return data

def connect():
    user = 
    password = 
    host_product = 
    dbname = 
    port = 

    product_connection_string = "dbname={dbname} user={user} host={host} password={password} port={port}"\
                                .format(dbname=dbname,
                                        user=user,
                                        host=host_product,
                                        password=password,
                                        port=port)
    try:
        product = pg.connect(product_connection_string)
    except:
        print('*****ERROR******')

        pc = product.cursor()
    return product

def map_dept_cd(data, mapping, dept_cd='major_cd'):
    mapping_ = mapping.set_index('dept_cd_before')['dept_cd_now'].to_dict()
    for old, new in tqdm(mapping_.items()):
        data[dept_cd] = data[dept_cd].str.replace(old, new)
    print("#------Mapping Completed!------#")
    return data

def cal_index_by_yr(gpa, year, top_n=100):
    gpa = gpa[gpa['yr']==year]
    gpa['credit'] = gpa['credit'].astype(float)
    gpa['gpa'] = gpa['gpa'].astype(float)
    df = gpa.groupby(['major_cd','dept_cd']).count().reset_index()[['major_cd','dept_cd','std_id']]
    df_piv = df.pivot(index='major_cd',columns='dept_cd', values='std_id').fillna(0)
    cos = pd.DataFrame(cosine_similarity(df_piv.T), index=df_piv.T.index, columns=df_piv.T.index)
    cos = pd.DataFrame(cos.stack()).reset_index(level=0).rename(columns={'dept_cd':'major_cd', 0:'dist'}).reset_index()
    cos['dist'] = (1-cos['dist']).round(5)
    gpa = pd.merge(gpa, cos, how ='left', on =['major_cd','dept_cd'])
    gpa['int_index']=gpa['gpa']*gpa['credit']*gpa['dist']
    gpa=gpa.groupby(['std_id']).mean().sort_values(by ='int_index', ascending=False).reset_index().iloc[:top_n]
    return gpa


def main(args):
    product = connect()
    gpa = data_load("./sql/index_gpa.txt", product)
    std = data_load("./sql/std_info.txt", product)
    mapping = data_load("./sql/dept_map.txt", product)

    gpa = map_dept_cd(gpa, mapping, 'major_cd')
    gpa = map_dept_cd(gpa, mapping, 'dept_cd')
    
    final_index = pd.DataFrame(columns=['std_id','gpa','credit','dist','int_index'])
    for i in tqdm(gpa.yr.unique().tolist()):
        data = cal_index_by_yr(gpa, i, -1)
        final_index = final_index.append(data, ignore_index=True) 
    
    final_in = final_index.groupby('std_id').mean().sort_values(by='int_index', ascending=False).reset_index()
    final_in = pd.merge(std, final_in).sort_values(by='int_index', ascending=False)
    final_in = final_in[final_in['rec_sts'] =='재학'] #enrolled students filtering
    final_in = final_in[~final_in['mmajor_nm'].isin(['다전공없음','심화전공'])].iloc[:args.top_n][['std_id','int_index']] #exclude students who have no 2nd major or advanced major
    
  
    del final_index
    print(final_in)
    
    final_in.to_csv("itd_index_"+datetime.now().strftime("%Y-%m-%d")+".txt", sep='\t', index=False)
    return final_in


if __name__ == '__main__':
   
    parser = argparse.ArgumentParser(description='Interdisiciplinary INDEX')
    
    parser.add_argument("--top_n", type=int, default=50)
    
    args = parser.parse_args()
    print(args)

    main(args)