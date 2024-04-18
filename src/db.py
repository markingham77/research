"""
queries.py is the module which write out the view_ids
"""

import click
import pydqt
import pandas as pd
# import snowflake.connector


# def create_tables():
#     """
#     creates SQL tables for embeddings, view_id and blocks of dates already called
#     """
#     pydqt.Query("""
#     CREATE TABLE QUERY_DATE_BLOCKS (
#                 start DATE,
#                 end DATE
#     )
#                 """)
#     # date blocks

def create_query_embeddings_table():
    """
    creates SQL table for embeddings
    """
    pydqt.Query("""
    CREATE TABLE QUERY_EMBEDDINGS (
                query VARCHAR,
                embedding VARIANT,
                embedding_length INT
    )
                """)
    # date blocks


def write_queries_data(min_query_date='2024-02-01',max_query_date='2024-02-05', run=False):
    """
    writes out SQL result of query terms and session_ids.
    This table is then joined with union_touch_points to assess
    further engagement
    """
    pydqt.set_workspace('/Users/mingham/research/workspaces/','search_and_recs')
    q=pydqt.Query('queries.sql',min_query_date=min_query_date,max_query_date=max_query_date)
    if run:
        print('running...')
        q.run()
    else:
        print('loading...')
        q.load()
    search_queries = pd.DataFrame(q.df['SEARCH_QUERY'].apply(lambda x: ' '.join(eval(x))))
    search_queries['VIEW_ID'] = q.df['VIEW_ID']
    search_queries=search_queries.dropna(how='any')
    # save queries to local 

    print(q.df.head())
    print(search_queries.head())

    search_queries.to_csv(f'/Users/mingham/research/workspaces/search_and_recs/cache/series_queries_with_view_id__{min_query_date}__{max_query_date}.csv')
    pd.Series(search_queries['SEARCH_QUERY'].unique()).to_csv(f'/Users/mingham/research/workspaces/search_and_recs/cache/distinct_queries__{min_query_date}__{max_query_date}.csv', index=False, header=False)


def write_queries_embeddings(min_query_date='2024-02-01', max_query_date='2024-02-05', run=False):
    """
    writes out:
        - queries and corresponding view_ids to a csv file
        - embeddings for distinct queries to CORE_WIP
    """
    # get the queries 
    pydqt.set_workspace('/Users/mingham/research/workspaces/','search_and_recs')
    q=pydqt.Query('queries.sql',min_query_date=min_query_date,max_query_date=max_query_date)
    if run:
        print('running...')
        q.run()
    else:
        print('loading...')
        q.load()
    search_queries = pd.DataFrame(q.df['SEARCH_QUERY'].apply(lambda x: ' '.join(eval(x))))
    search_queries['VIEW_ID'] = q.df['VIEW_ID']
    search_queries=search_queries.dropna(how='any')
    # write out search queries and corresponding view_ids
    search_queries.to_csv(f'/Users/mingham/research/workspaces/search_and_recs/cache/series_queries_with_view_id__{min_query_date}__{max_query_date}.csv')

    # get distinct queries
    these_distinct_queries = pd.Series(search_queries['SEARCH_QUERY'].unique())

    # insert into CORE_WIP
    pydqt.Query('''
    INSERT INTO lyst.core_wip.query_embeddings (column1, column2, ...)
    SELECT value1, value2, ...
    FROM your_source_table
    LEFT JOIN your_table ON your_source_table.unique_column = your_table.unique_column
    WHERE your_table.unique_column IS NULL;
    ''')


@click.command()
@click.option('--run', is_flag=True, default=False, help='Force pydqt to run rather than load')
@click.option('--min_query_date', default='2024-02-01', help="start SQL query date, uses event_timestamp")
@click.option('--max_query_date', default='2024-02-05', help="end SQL query date, uses event_timestamp")
def cli(min_query_date, max_query_date, run):
    write_queries_data(min_query_date, max_query_date, run)

if __name__=='__main__':
    cli()