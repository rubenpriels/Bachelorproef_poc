import keras
import tensorflow as tf
import pandas as pd
import numpy as np
from tensorflow.keras.layers import Embedding, Input, Flatten
from tensorflow.keras.models import Model

# Scale data
# hotencode data
# 

class SequenceDataset:
    # BELANGRIJK:
    # BIJ DE DATASET KIJKEN: HOEVEEL RIJEN HEEFT ELKE WINKEL?
    # DE WINKELS MET MINDER DAN X AANTAL RIJEN -> VERWIJDEREN

    # https://chatgpt.com/c/69db5e03-1b90-8332-bc41-dfc1fa6952e4
    # EERST ONE HOT ENCODING -> DAN PAS STANDARD SCALING

    def __init__(self, dataset, sequence_length, batch_size, column, predict_column_index): # , categorical_columns
        self.dataset = dataset
        self.batch_size = batch_size
        self.sequence_length = sequence_length
        self.column = column
        self.predict_column_index = predict_column_index
    

    #             winkel_id ──► embedding ┐
    #                                     ├──► concat ─► dense layers
    #  prijs ──► standard scaler ────────┘
    
    # 48 dimensies voor embedding
    
    def make_sequence_dataset(self):
        df = self.__embed_dataframes(self.dataset, self.column)
        dfs = self.__split_dataframe(df, self.column)
        ds = self.__sliding_windows(dfs, self.batch_size, self.sequence_length, self.predict_column_index)
        train_data, val_data, test_data = self.__split_dataset(ds)
        return train_data, val_data, test_data
    
    def make_sequence_dataset(self):
        df = self.dataset

        # split eerst dataframe (time-based!)
        train_df, val_df, test_df = self.__split_dataframe_time(df)

        # fit scaler op train
        train_df, val_df, test_df = self.__embed_dataframes(train_df, val_df, test_df)

        # split per locatie
        train_dfs = self.__split_dataframe(train_df, self.column)
        val_dfs   = self.__split_dataframe(val_df, self.column)
        test_dfs  = self.__split_dataframe(test_df, self.column)

        # sliding windows
        train_ds = self.__sliding_windows(train_dfs, self.batch_size, self.sequence_length, self.predict_column_index)
        val_ds   = self.__sliding_windows(val_dfs, self.batch_size, self.sequence_length, self.predict_column_index)
        test_ds  = self.__sliding_windows(test_dfs, self.batch_size, self.sequence_length, self.predict_column_index)

        return train_ds, val_ds, test_ds
    
    def __embed_dataframes(self, df: list[pd.DataFrame], column: str) -> list[pd.DataFrame]:
        # MAAK HIER EVEN HOTONENCODING VAN
        return df
    
    def __split_dataframe(self, df: pd.DataFrame, column: str) -> list[pd.DataFrame]:
        dfs = [group for _, group in df.groupby(column)]
        return dfs
    
    def __combine_datasets(self, datasets):
        ds_all = datasets[0]
        for ds in datasets[1:]:
            ds_all = ds_all.concatenate(ds)
        return ds_all
    
    def __sliding_windows(self, dfs, batch_size, sequence_length, predict_column_index):
        # stap 1: GEDAAN IN VORIGE STAPPEN
        # split de dataframe in meerdere dataframes op basis van de column variabel
        # dus df1 = winkel A, df2 = winkel B, df3 = winkel C

        # stap 1: dataframes moeten worden omgezet in numpy

        # stap 2: maak een for lus die elke dataframe door deze timeseries_dataset_from_array laat gaan.
        datasets = []

        for df in dfs:
            x = df.to_numpy(dtype=np.float32)
            y = df[df.columns[predict_column_index]].to_numpy(dtype=np.float32)

            ds = keras.utils.timeseries_dataset_from_array(
                x,
                targets=y[sequence_length:],
                sequence_length=sequence_length,
                sequence_stride=1,
                batch_size=batch_size
            )

            datasets.append(ds)

        ds = self.__combine_datasets(datasets)
        return ds
    
    def __split_dataset(self, ds):
        ds = ds.shuffle(1000, reshuffle_each_iteration=False)

        n = len(list(ds))
        train_size = int(0.7 * n)
        val_size = int(0.15 * n)

        train_data = ds.take(train_size)
        val_data = ds.skip(train_size).take(val_size)
        test_data = ds.skip(train_size + val_size)
        return train_data, val_data, test_data


























class DataframeCleaner:

    def clean_dataframe(self, df):
        df = self.__basic_cleaner(df)
        return df

    def __basic_cleaner(self, df):
        # drop duplicates etc
        df = df.drop_duplicates(inplace=True)

        # misschien kijken dat als slechts 10% van de data NA is, dan het vervangen door de vorige waarde/gemiddelde?
        # wat als de vorige waarde van een andere locatie is tho?
        # denk aan tv series. 
        df = df.dropna()

        # convert to the right datatype

        return df