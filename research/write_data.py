from datetime import datetime
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
MAX_QUERY_DATE='2024-03-01'

def random_df(n=3, rows=10):
    """
    produces a random dataframe
    """
    data = {f'Column_{i+1}': np.random.rand(rows) for i in range(n)}
    df = pd.DataFrame(data)    
    return df


def nc_components(min_query_date='2018-01-01',max_query_date=MAX_QUERY_DATE,freq='',append=False):
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

def write_from_template(template,min_query_date='2018-01-01',max_query_date=MAX_QUERY_DATE,freq='',append=False, schema='CORE_WIP',**kwargs):

    def convert_to_date(input_string, date_format = '%Y-%m-%d:%H-%M-%S', return_date=True):
        # Use datetime.strptime to parse the input string and convert it to a datetime object
        date_object = datetime.strptime(input_string, date_format)        
        # Return the datetime object
        if return_date:
            return date_object.date()
        return date_object
    
    def convert_to_datetime(input_string, date_format = '%Y-%m-%d:%H-%M-%S'):
        return convert_to_date(input_string, date_format = '%Y-%m-%d:%H-%M-%S',return_date=False)


    if freq:
        q = pydqt.Query(template,min_query_date=min_query_date,max_query_date=max_query_date, freq=freq)
    else:
        q = pydqt.Query(template,min_query_date=min_query_date,max_query_date=max_query_date)
    q.run(schema=schema)

    df=q.df
    if kwargs:
        for key in kwargs:
            if kwargs[key].upper()=='DATE':
                # ENSURE UNDERLYING DATA IS DATE OBJECT, otherwise q.write_sql() will complain
                if key in df.columns:
                    if type(df[key][0]) == str:
                        df[key] = df[key].apply(convert_to_date)      
            elif kwargs[key].upper()=='DATETIME':
                if key in df.columns:
                    if type(df[key][0]) == str:
                        df[key] = df[key].apply(convert_to_datetime)      
    q.df=df                    

    table_name = template.lower().replace('.sql','')
    print(q.df.head())
    if freq:
        f=freq[0]
        q.write_sql(f'{table_name}_{f}',schema='CORE_WIP',append=append, **kwargs)
    else:
        q.write_sql(table_name,schema='CORE_WIP',append=append, **kwargs)

def test(rows=10, append=True):
    q = pydqt.Query(query="select * from '{{table}}' limit {{rows}};",table=pydqt.test_data_file_full_path(),rows=str(rows))
    q.run()
    q.write_sql('test',schema='CORE_WIP', append=append)


@click.command()
@click.option('--rows', default=10)
@click.option('--start', default='2018-01-01', help='start query date')
@click.option('--end', default=MAX_QUERY_DATE, help='end query date')
@click.option('--event_ds', default='DATE', help='forcing event_ds field to be a date')
@click.option('--freq', default='', help='frequency of NC data')
@click.option('--schema', default='CORE_WIP', help='schema')
@click.option('--append', is_flag=True, default=True, help='append data to existing table')
@click.option('--overwrite', is_flag=True, default=False, help='overwrite data in existing table')
# @click.option('--template',default='',help='name of template (will be in your current workspace)')
@click.argument('template')
def cli(template,start,end,rows,freq,schema,append,overwrite,event_ds):
    if overwrite:
        append=False
    # if template.lower()=='nc_components':
    #     if freq != '':
    #         freq=[freq]
    #         freq = [f.upper() for f in freq]    
    #         nc_components(min_query_date=start, max_query_date=end,freq=freq, append=append)
    #     else:
    #         nc_components(min_query_date=start, max_query_date=end, append=append)
    if template.lower()=='test':
        test(rows=rows)
    else:
        if freq != '':
            freq=[freq]
            freq = [f.upper() for f in freq]    
            write_from_template(template,min_query_date=start, max_query_date=end,freq=freq, append=append, schema=schema, EVENT_DS=event_ds)    
        else:
            write_from_template(template,min_query_date=start, max_query_date=end, append=append, schema=schema, EVENT_DS=event_ds)

if __name__=='__main__':
    cli()