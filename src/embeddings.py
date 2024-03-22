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
    search_queries['SESSION_ID'] = q.df['SESSION_ID']
    search_queries=search_queries.dropna(how='any')
    # save queries to local 
    search_queries.to_csv('/Users/mingham/research/workspaces/search_and_recs/cache/series_queries_with_session_id.csv')


def _rename_embeddings():
    """
    An unfortunately necessary function to rename some files that were labelled incorrectly by
    calculate_embeddings - a bug which is now fixed.
    """
    pass

# @click.command()
# @click.option("--chunk_size", default=100, help="Number of documents to embed.")
def calculate_embeddings(list_of_queries, epochs=10, chunk_size=100, number_of_runs=3):    
    """
    computes the embeddings of a list of queries.  This uses the Fashion version of CLIP and only exrtacts the text embeddings.
    Embeddings are written to a csv file.
    """
    # fobj = open(EMBEDDINGS_FILE, "wb+")
    
    L = len(list_of_queries)
    upper_lim = math.floor(L/chunk_size)
    if number_of_runs:
        upper_lim = number_of_runs
    N = range(0,max(1,upper_lim))    
    all_embeds=[]
    
    cnt=0    
    for epoch in range(0,epochs+1):
        cnt+=1
        # get last epoch
        last_epoch_df = pd.read_csv(EMBEDDINGS_INFO_FILE)
        epoch = last_epoch_df['last_epoch'].values[0]+1
        
        embeddings_file = EMBEDDINGS_FILE.replace('.csv',f'_{epoch}.csv')
        csv_file_obj = open(embeddings_file, "w+")
        writer = csv.writer(csv_file_obj, delimiter=',')

        append = False

        for chunk_idx in N:
            print(f'{chunk_idx}/{upper_lim} in Epoch {epoch}/{epochs}')
            tracemalloc.start()
            current, peak = tracemalloc.get_traced_memory()
            print('Starting Current and peak memory usage: {}MB {}MB'.format(current/(1024*1024), peak/(1024*1024)))

            start_idx = chunk_idx*chunk_size + chunk_size*epoch
            end_idx = start_idx+chunk_size
            print(start_idx, end_idx, chunk_size)
            chunk_of_queries = list_of_queries[start_idx:end_idx]

            inputs = tokenizer(chunk_of_queries, padding=True, return_tensors="pt")
            # inputs = tokenizer(list_of_queries, padding=True, return_tensors="pt")
            current, peak = tracemalloc.get_traced_memory()
            print('Inputs Current and peak memory usage: {}MB {}MB'.format(current/(1024*1024), peak/(1024*1024)))

            try:
                text_embeds = model.get_text_features(**inputs).detach().cpu().numpy()
                # print(text_embeds.tolist()[1])
                # print(','.join(text_embeds.tolist()))
                column_names = [f'feature_{i}' for i in range(1, len(text_embeds[0]) + 1)]
                df = pd.DataFrame(data=text_embeds,columns=column_names)
                df['query'] = chunk_of_queries
                df=df[['query'] + column_names]
                
                if append:
                    df.to_csv(csv_file_obj, index=False, header=False)
                else:
                    df.to_csv(csv_file_obj, index=False, header=True) 
                    append=True   

                # for (idx, v) in enumerate(chunk_of_queries):
                #     print('idx:',idx,v)
                #     line = f'"{v}",' + ','.join(str(text_embeds.tolist()[idx]))
                #     writer.writerow(line)

                # result = dict(zip(chunk_of_queries, text_embeds))
            except:
                print('issue with model.get_text_features')
            print('Outputs Current and peak memory usage: {}MB {}MB'.format(current/(1024*1024), peak/(1024*1024)))
                # pickle.dump(result,fobj)


        csv_file_obj.close()
        last_epoch_df['last_epoch']=epoch
        last_epoch_df.to_csv(EMBEDDINGS_INFO_FILE,index=False)
    return 

# def load_embeddings():
#     """
#     loads in pickle file which contains the query embeddings
#     """
#     fobj = open(EMBEDDINGS_FILE, "rb")
#     embeddings = pickle.load(fobj)
#     fobj.close()
#     return embeddings




@click.command()
@click.option('--epochs', default=100)
@click.option('--chunk_size', default=200)
@click.option('--number_of_runs', default=50)
@click.option('--write', is_flag=True, default=False, help='append data to existing table')
def cli(chunk_size, write, number_of_runs, epochs):    
    # print('load is',load)
    # print('write is',write)
    if write:
        df = pd.read_csv('/Users/mingham/research/workspaces/search_and_recs/cache/series_queries.csv')
        calculate_embeddings(df['QUERY'].to_list(), epochs=epochs, chunk_size=chunk_size, number_of_runs=number_of_runs)

if __name__=='__main__':
    cli()
