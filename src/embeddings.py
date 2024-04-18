import csv
import pickle
import click
import math
from memory_profiler import profile
import pandas as pd
from transformers import AutoTokenizer, AutoModel, CLIPTextModelWithProjection 
import tracemalloc
from pathlib import Path
import os
import pydqt

# # general CLIP model
# model = CLIPTextModelWithProjection.from_pretrained("openai/clip-vit-base-patch32")
# tokenizer = AutoTokenizer.from_pretrained("openai/clip-vit-base-patch32")

#Open file to store the embeddings
EMBEDDINGS_FILE = os.path.join(Path(__file__).parents[0], "embeddings.csv")
EMBEDDINGS_INFO_FILE = os.path.join(Path(__file__).parents[0], "embeddings_info.csv")

model_name = "patrickjohncyh/fashion-clip"
model = AutoModel.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)

def calculate_embeddings_from_queries_sql(min_query_date='2024-01-01',max_query_date='2024-04-05', number_of_records=100000, chunk_size=100):
    """
    queries.sql will return a specified number of (random) records from within the min and max dates.  Note that there may 

    chunk_size controls how many queries (default=100) are sent to the FashionCLIP model to be embedded

    """
    q=pydqt.Query('queries_for_encoding.sql',min_query_date=min_query_date,max_query_date=max_query_date)
    q.run()
    q.df['QUERY']=q.df['SEARCH_QUERY'].apply(lambda x: ' '.join(eval(x)))
    search_queries_with_view_id = q.df[['QUERY']]
    search_queries_with_view_id=search_queries_with_view_id.dropna(how='any')
    list_of_queries = search_queries_with_view_id['QUERY'].unique().tolist()
    list_of_distinct_queries = list(set(list_of_queries))

    tracemalloc.start()
    current, peak = tracemalloc.get_traced_memory()
    print('Starting Current and peak memory usage: {}MB {}MB'.format(current/(1024*1024), peak/(1024*1024)))

    # for testing
    chunk_of_queries = list_of_distinct_queries[:10]
    inputs = tokenizer(chunk_of_queries, padding=True, return_tensors="pt")
    text_embeds = model.get_text_features(**inputs).detach().cpu().numpy()
    # print(text_embeds)
    # print(text_embeds.tolist())
    df = pd.DataFrame(data=[','.join(map(str,x)) for x in text_embeds.tolist()],columns=['embedding'])
    df['query'] = chunk_of_queries
    query=pydqt.Query('select')
    query.df=df
    # print(df.head())
    query.write_sql('search_query_embeddings',schema='CORE_WIP',database='lyst',append=True, unique='query')


@click.command()
@click.option('--min_query_date', default='2024-02-01', help="start SQL query date, uses event_timestamp")
@click.option('--max_query_date', default='2024-02-05', help="end SQL query date, uses event_timestamp")
def cli(chunk_size, write, number_of_runs, epochs, min_query_date, max_query_date):
    calculate_embeddings_from_queries_sql(min_query_date=min_query_date,max_query_date=max_query_date)
if __name__=='__main__':
    cli()
