import click
import sys
import snowflake.connector
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

import pydqt
import itertools
import os

import numpy as np
import altair as alt

import pandas as pd
import os 


pydqt.set_workspace(root='/Users/mingham/research/workspaces/',name='growth_model')

def random_df(n=3, rows=10):
    """
    produces a random dataframe
    """
    data = {f'Column_{i+1}': np.random.rand(rows) for i in range(n)}
    df = pd.DataFrame(data)    
    return df


def check_aggregates():
    """
    checks that aggregates equals aggregated sub-components
    """
    dimensions = ["major_market_partner", "is_member", "user_type", "session_traffic_source_grouping"]        
    q=pydqt.Query('select * from nc_components_all_in_one')



def nc_components(min_query_date='2018-01-01',max_query_date='2024-01-01',freq='',append=False):
    if freq:
        q = pydqt.Query('nc_components.sql',min_query_date=min_query_date,max_query_date=max_query_date, freq=freq)
    else:
        q = pydqt.Query('nc_components.sql',min_query_date=min_query_date,max_query_date=max_query_date)
    q.run()
    if freq:
        f=freq[0]
        q.write_sql(f'nc_components_{f}',schema='CORE_WIP',append=append)
    else:
        q.write_sql('nc_components',schema='CORE_WIP',append=append)

def test(rows=10, append=True):
    q = pydqt.Query(query="select * from '{{table}}' limit {{rows}};",table=pydqt.test_data_file_full_path(),rows=str(rows))
    q.run()
    q.write_sql('test',schema='CORE_WIP', append=append)


@click.command()
@click.option('--rows', default=10)
@click.option('--start', default='2018-01-01', help='start query date')
@click.option('--end', default='2024-01-01', help='end query date')
@click.option('--freq', default='', help='frequency of NC data')
@click.option('--append', is_flag=True, default=False, help='append data to existing table')
@click.argument('name')
def cli(name,start,end,rows,freq,append):
    if name.lower()=='nc_components':
        if freq != '':
            freq=[freq]
            freq = [f.upper() for f in freq]    
            nc_components(min_query_date=start, max_query_date=end,freq=freq, append=append)
        else:
            nc_components(min_query_date=start, max_query_date=end, append=append)
    elif name.lower()=='test':
        test(rows=rows)

if __name__=='__main__':
    cli()