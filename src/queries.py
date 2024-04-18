"""
queries.py is the module which write out the view_ids
"""

import click
import pydqt
import pandas as pd





def write_queries_data(min_query_date='2023-08-01',max_query_date='2023-08-15'):
    """
    writes out SQL result of query terms and session_ids.
    This table is then joined with union_touch_points to assess
    further engagement
    """
    pydqt.set_workspace('/Users/mingham/research/workspaces/','search_and_recs')
    q=pydqt.Query('queries.sql',min_query_date=min_query_date,max_query_date=max_query_date)
    q.load()
    search_queries = q.df['SEARCH_QUERY'].apply(lambda x: ' '.join(eval(x)))
    search_queries['VIEW_ID'] = q.df['VIEW_ID']
    search_queries=search_queries.dropna(how='any')
    # save queries to local 

    search_queries.to_csv('/Users/mingham/research/workspaces/search_and_recs/cache/series_queries_with_view_id.csv')


@click.command()
@click.option('--min_query_date', default='2024-02-01', help="start SQL query date, uses event_timestamp")
@click.option('--max_query_date', default='2024-02-05', help="end SQL query date, uses event_timestamp")
def cli(min_query_date, max_query_date):
    write_queries_data(min_query_date, max_query_date)
