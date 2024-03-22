import matplotlib.cm as cm
import os
import pandas as pd
import snowflake.connector
import traceback
import matplotlib.pyplot as plt
import pickle
import numpy as np
import ast
import torch
import requests
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.metrics import silhouette_samples, silhouette_score
from sklearn.neighbors import NearestNeighbors
import sklearn as sk
from pathlib import Path
from io import BytesIO
from transformers import AutoProcessor, AutoModel, AutoTokenizer, CLIPTextModelWithProjection 
from typing import Callable, Dict, Iterator
from matplotlib import pyplot as plt
from scipy.cluster.hierarchy import dendrogram

# from PIL import Image
# from annoy import AnnoyIndex
# from torchvision import transforms

import pydqt


import random
import math

# import local embeddings code
import sys
sys.path.append('../src/')
import embeddings



# set workspace
pydqt.set_workspace('/Users/mingham/research/workspaces/','search_and_recs')
#Open file to store the embeddings
EMBEDDINGS_FILE = os.path.join(Path(__file__).parents[0], "embeddings.csv")
EMBEDDINGS_INFO_FILE = os.path.join(Path(__file__).parents[0], "embeddings_info.csv")



def plot_dendrogram(model, **kwargs):
    """
    plots dendrogram of full tree (see "compute_hierachical_clusters", below - set full_tree=True)
    """
    # Create linkage matrix and then plot the dendrogram

    # create the counts of samples under each node
    counts = np.zeros(model.children_.shape[0])
    n_samples = len(model.labels_)
    for i, merge in enumerate(model.children_):
        current_count = 0
        for child_idx in merge:
            if child_idx < n_samples:
                current_count += 1  # leaf node
            else:
                current_count += counts[child_idx - n_samples]
        counts[i] = current_count

    linkage_matrix = np.column_stack(
        [model.children_, model.distances_, counts]
    ).astype(float)
    # Plot the corresponding dendrogram
    dendrogram(linkage_matrix, **kwargs)
    plt.xlabel("Number of points in node (or index of point if no parenthesis).")
    plt.show()


def compute_hierachical_clusters(df,column_names, full_tree=False):
    """
    hierachical kmeans using sklearn Agglomerative clustering
    This is basically an opinionated call to the kmeans algo and
    takes a dataframe as input
    """
    if full_tree:
        return AgglomerativeClustering(n_clusters=None, distance_threshold=0).fit(df[column_names].to_numpy())    
    return AgglomerativeClustering(n_clusters=10, distance_threshold=0).fit(df[column_names].to_numpy())

def compute_clusters(df,column_names):
    """
    traditional kmeans
    """
    clusterer = KMeans(n_clusters=10, random_state=10)
    cluster_labels = clusterer.fit_predict(df[column_names].to_numpy())
    return cluster_labels


def get_embeddings(list_of_queries=[]):
    """
    get embeddings from the fashion CLIP model.  This is the CLIP model, fine-tuned for fashion terms.  
    The more genaral CLIP model can be found here:

    from transformers import AutoTokenizer, CLIPTextModelWithProjection     
    model = CLIPTextModelWithProjection.from_pretrained("openai/clip-vit-base-patch32")
    tokenizer = AutoTokenizer.from_pretrained("openai/clip-vit-base-patch32")

    """
    model_name = "patrickjohncyh/fashion-clip"
    model = AutoModel.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    inputs = tokenizer(list_of_queries, padding=True, return_tensors="pt")
    embeddings = model.get_text_features(**inputs).detach().cpu().numpy()
    return embeddings


def plot_single_page(start_idx,df,column_names,sup=False, layout=[2,2], range_n_clusters = [2, 5,7,8,9,10,11,12, 15, 20, 30, 40,80,100,200,400, 600, 800, 1600, 2400]):
    if layout==None:
        pass

    X=df[column_names].to_numpy()

    if type(layout)==int:
        layout=[layout, 1]

    # fig, axes = plt.subplots(2, 2)
    fig, axes = plt.subplots(layout[0], layout[1])

    # print(axes[0])
    fig.set_size_inches(18, 7)
    axes_2 = [axes[0][0], axes[0][1], axes[1][0], axes[1][1]]
    inertia = []
    for idx,ax in enumerate(axes_2):
        n_clusters = range_n_clusters[start_idx+idx]
        ax.set_xlim([-0.1, 1])
        ax.set_ylim([0, len(X) + (n_clusters + 1) * 10])
    
        # Initialize the clusterer with n_clusters value and a random generator
        # seed of 10 for reproducibility.
        clusterer = KMeans(n_clusters=n_clusters, random_state=10)
        cluster_labels = clusterer.fit_predict(X)
        errors = clusterer.inertia_
        inertia.append(errors)

    # The silhouette_score gives the average value for all the samples.
    # This gives a perspective into the density and separation of the formed

        # clusters
        silhouette_avg = silhouette_score(X, cluster_labels)
        print(
            "For n_clusters =",
            n_clusters,
            "The average silhouette_score is :",
            silhouette_avg,
        )

        # Compute the silhouette scores for each sample
        sample_silhouette_values = silhouette_samples(X, cluster_labels)

        y_lower = 10
        for i in range(n_clusters+1):
            # Aggregate the silhouette scores for samples belonging to
            # cluster i, and sort them
            ith_cluster_silhouette_values = sample_silhouette_values[cluster_labels == i]

            ith_cluster_silhouette_values.sort()

            size_cluster_i = ith_cluster_silhouette_values.shape[0]
            y_upper = y_lower + size_cluster_i

            color = cm.nipy_spectral(float(i) / n_clusters)
            ax.fill_betweenx(
                np.arange(y_lower, y_upper),
                0,
                ith_cluster_silhouette_values,
                facecolor=color,
                edgecolor=color,
                alpha=0.7,
            )

            # Label the silhouette plots with their cluster numbers at the middle
            ax.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))

            # Compute the new y_lower for next plot
            y_lower = y_upper + 10  # 10 for the 0 samples

        ax.set_title(f"N clusters = {i}")
        if idx in [2,3]:
            ax.set_xlabel("The silhouette coefficient values")
        ax.set_ylabel("Cluster label")

        # The vertical line for average silhouette score of all the values
        ax.axvline(x=silhouette_avg, color="red", linestyle="--")

        ax.set_yticks([])  # Clear the yaxis labels / ticks
        ax.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])


    if sup:
        plt.suptitle("Silhouette analysis for KMeans clustering on Fashion CLIP embeddings of queries",                
            fontsize=14,
            fontweight="bold",
        )

    return inertia    

def plot_silhouettes(X, clusters=[2, 5, 7, 8]):

    layout = [2,2]
    if type(clusters)==int:
        clusters=[clusters]
    if len(clusters)==1:
        layout=[1,1]
    elif len(clusters) in [2,3]:
        layout=[len(clusters),1]
    else:
        if len(clusters)>4:
            print('Warning: plot_silouettes will only plot a maximum of 4 clusters.  Call it in a loop if you need more')

    # range_n_clusters=[2, 5,7,8, 9,10,11,12, 15, 20, 30, 40, 60, 80,100, 200]
    range_n_clusters = clusters
    # fig, axes = plt.subplots(2, 2)
    fig, axes = plt.subplots(layout[0], layout[1])

    # print(axes[0])
    fig.set_size_inches(18, 7)
    axes_2 = [axes[0][0], axes[0][1], axes[1][0], axes[1][1]]
    inertia = []
    for idx,ax in enumerate(axes_2):
        n_clusters = range_n_clusters[idx]
        ax.set_xlim([-0.1, 1])
        ax.set_ylim([0, len(X) + (n_clusters + 1) * 10])
    
        # Initialize the clusterer with n_clusters value and a random generator
        # seed of 10 for reproducibility.
        clusterer = KMeans(n_clusters=n_clusters, random_state=42, init="k-means++", n_init=50, max_iter=500)
                              
        cluster_labels = clusterer.fit_predict(X)
        errors = clusterer.inertia_
        inertia.append(errors)

    # The silhouette_score gives the average value for all the samples.
    # This gives a perspective into the density and separation of the formed

        # clusters
        silhouette_avg = silhouette_score(X, cluster_labels)
        print(
            "For n_clusters =",
            n_clusters,
            "The average silhouette_score is :",
            silhouette_avg,
        )

        # Compute the silhouette scores for each sample
        sample_silhouette_values = silhouette_samples(X, cluster_labels)

        y_lower = 10
        for i in range(n_clusters+1):
            # Aggregate the silhouette scores for samples belonging to
            # cluster i, and sort them
            ith_cluster_silhouette_values = sample_silhouette_values[cluster_labels == i]

            ith_cluster_silhouette_values.sort()

            size_cluster_i = ith_cluster_silhouette_values.shape[0]
            y_upper = y_lower + size_cluster_i

            color = cm.nipy_spectral(float(i) / n_clusters)
            ax.fill_betweenx(
                np.arange(y_lower, y_upper),
                0,
                ith_cluster_silhouette_values,
                facecolor=color,
                edgecolor=color,
                alpha=0.7,
            )

            # Label the silhouette plots with their cluster numbers at the middle
            ax.text(-0.05, y_lower + 0.5 * size_cluster_i, str(i))

            # Compute the new y_lower for next plot
            y_lower = y_upper + 10  # 10 for the 0 samples

        q25, q50, q75 = np.percentile(sample_silhouette_values, [25, 50, 75])
        q25=np.round(q25,2)
        q50=np.round(q50,2)
        q75=np.round(q75,2)

        ax.set_title(f"N clusters = {i}, 25th={q25}, median={q50}, 75th={q75}")
        if idx in [2,3]:
            ax.set_xlabel("The silhouette coefficient values")
        ax.set_ylabel("Cluster label")

        # The vertical line for average silhouette score of all the values
        ax.axvline(x=silhouette_avg, color="red", linestyle="--")

        ax.set_yticks([])  # Clear the yaxis labels / ticks
        ax.set_xticks([-0.1, 0, 0.2, 0.4, 0.6, 0.8, 1])

def create_cluster_report(indf):
    range_n_clusters = [2, 5,7,8,9,10,11,12, 15, 20, 30, 40,80,100,200,400, 600, 800, 1600, 2400]
    df, column_names = create_ortho_embeddings(indf)
    df=df.dropna()
    if df.shape[0]>0:
        df = df.reset_index(drop=True)

        # X=df[column_names].to_numpy()
        inertias = []
        for page in range(0,5):
            # Create a subplot with 2 rows and 2 columns
            sup=False
            if page==0:
                sup=True        
            errors = plot_single_page(page*4,df,column_names,sup)
            inertias = inertias + errors
        plt.show()
        return inertias
    
def plot_inertia(inertias):
    range_n_clusters = [2, 5,7,8,9,10,11,12, 15, 20, 30, 40,80,100,200,400, 600, 800, 1600, 2400]
    plt.plot(range_n_clusters, inertias, marker='o');
    plt.xlabel('Number of clusters');
    plt.ylabel('Inertia');

def get_random_samples(sample_size = 100000):
    csv_files = ['/Users/mingham/research/src/' + x for x in os.listdir('/Users/mingham/research/src/') if x[-4:] == '.csv']    
    n_files = min(math.floor(sample_size/10000),len(csv_files))
    chosen_files = random.sample(csv_files, n_files)
    for (idx, fn) in enumerate(chosen_files):
        if idx==0:
            df = pd.read_csv(fn)
        else:
            df = pd.concat([df, pd.read_csv(fn)],axis=0)    
    return df


def ortho(X):
    # create orthornmal embeddings
    length = np.sqrt((X**2).sum(axis=1))[:,None]
    return X / length

def create_ortho_embeddings(embeddings_df):
    xx = embeddings_df.drop_duplicates(['query'])
    xx = xx.reset_index(drop=True)
    
    embeddings2 = ortho(xx[xx.columns[1:]].to_numpy())
    column_names = [f'feature_{i}' for i in range(1, embeddings2.shape[1] + 1)]
    embeddings2_df = pd.DataFrame(data=embeddings2,columns=column_names)
    # df_nodups = embeddings2_df.drop_duplicates(['query'])
    # df_nodups = df_nodups.reset_index(drop=True)
    embeddings2_df['query']=xx['query']
    df_nodups = embeddings2_df[['query']+column_names]
    return embeddings2_df, column_names