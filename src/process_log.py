#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# your Python code to implement the features could be placed here
# note that you may use any language, there is no preference towards Python
import pandas as pd
from datetime import datetime,timedelta
import os

prj_dir = os.path.dirname(os.path.dirname(__file__))

# data preparation
def extract_time(string):
    time = datetime.strptime(string,'[%d/%b/%Y:%H:%M:%S%z]')
    return time
    
def get_data():
    fpath = os.path.join(prj_dir,'log_input/log.txt')
    df = pd.read_table(fpath, sep = ' ',
                       encoding = 'ISO-8859-1',
                       header=None,
                       names=['host','','','time','zone','resource','http_reply','bytes'])
    df.time = df.time + df.zone
    df.drop(df.columns[[1,2]],axis=1,inplace=True)
    df['bytes'].replace('-',0, inplace=True)
    df.time = [extract_time(text) for text in df.time.values]
    return df

# challenge 1    
def most_frequent_hosts(df):
    sortedHosts = df.groupby('host').size().sort_values(ascending=False)[:10]
    fpath = os.path.join(prj_dir,'log_output/hosts.txt')
    with open(fpath,'w') as f:
        for host, count in zip(sortedHosts.index, sortedHosts.values):
            f.write('%s,%d\n' % (host,count))

# challenge 2            
def highest_demand_resources(df):
    df.bytes = df.bytes.astype('int')
    sortedResource = df[['resource','bytes']].groupby('resource').sum().\
                    sort_values(by='bytes',ascending=False)[:10]
    fpath = os.path.join(prj_dir, 'log_output/resources.txt')         
    with open(fpath,'w') as f:
        for rsc in sortedResource.index:
            splits = rsc.split()
            f.write('%s\n' % splits[1])
        
# challenge 3       
def busiest_hours(df):
    visits = {} # {start_time: visit counts}
    idx = 0    
    t_start = df.iloc[0].time
    while idx <= df.index[-1]:
        #print(t_start)
        t_end = t_start + timedelta(minutes=60)              
        ct = len(df[(df['time'] >= t_start) & (df['time'] <= t_end)])
        visits[t_start] = ct 
        t_start += timedelta(seconds=1)
        idx += 1            
        
    top10_hours = sorted(visits.items(),key = lambda x: (-x[1],x[0]))[:10]
    fpath = os.path.join(prj_dir,'log_output/hours.txt')
    with open(fpath,'w') as f:
        for item in top10_hours:
            f.write('%s,%d\n' % (datetime.strftime(item[0],\
            '%d/%b/%Y:%H:%M:%S %z'),item[1]))
        
# challenge 4
def reset_host(host, block_status):
    block_status[host][0] = None # resets fail time
    block_status[host][1] = 0 # resets fail counter
    block_status[host][2] = None # resets block time
    
def update_host(trial, block_status,f):    
    if trial[5] == 401:
        if trial[1] not in block_status: # if first time fail 
            block_status[trial[1]] = [trial[2], 0, None]
        else:
            if not block_status[trial[1]][0] or \
                trial[2] - block_status[trial[1]][0] <= \
                timedelta(seconds=20): # within 20s since last fail
                block_status[trial[1]][1] += 1
                if block_status[trial[1]][1] > 2:
                    block_status[trial[1]][2] = trial[2]
                    f.write('%s - - [%s] "%s" %d %d\n' % \
                            (trial[1], datetime.strftime(trial[2],\
                            '%d/%b/%Y:%H:%M:%S %z'), trial[4],trial[5], trial[6]))
            else: # more than 20s since last fail
                reset_host(trial[1],block_status)
    else:
        pass
        
def blocked_hosts(df): 
    block_status = {} # {host: [first_fail_time=None, fail_count=0, block_time=None]}
    fpath = os.path.join(prj_dir, 'log_output/blocked.txt')
    with open(fpath,'w') as f:            
        for trial in df.itertuples():
            print(trial[2])
            if trial[1] in block_status and block_status[trial[1]][2] != None: 
                if trial[2] - block_status[trial[1]][2] <= timedelta(minutes = 5):
                    f.write('%s - - [%s] "%s" %d %d\n' % \
                            (trial[1], datetime.strftime(trial[2],\
                            '%d/%b/%Y:%H:%M:%S %z'), trial[4],trial[5], trial[6]))
                else:
                    reset_host(trial[1],block_status)
                    update_host(trial,block_status, f)
            else:
                update_host(trial, block_status, f)
                
def main():
    print('Loading data...')
    df = get_data()
    print('calculating most frequent hosts...')
    most_frequent_hosts(df)
    print('calculating most demanded resource...')
    highest_demand_resources(df)
    print('calculating busiest hours...')
    busiest_hours(df)
    print('Blocking hosts with failed login...')
    blocked_hosts(df)
    
main()
    
    
    
