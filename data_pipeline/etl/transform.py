import pandas as pd
import numpy as np
import re


def clean_drugs_df(file_path: str) -> pd.DataFrame:
    """clean drugs.parquet"""

    df = pd.read_parquet(file_path)
    df.rename(
        columns={'atccode': 'id'},
        inplace=True
    )
    df['drug'] = df.drug.str.strip().str.lower()

    return df


def clean_pubmed_df(file_path: str) -> pd.DataFrame:
    """clean pubmed.parquet"""

    df = pd.read_parquet(file_path)
    df['date'] = pd.to_datetime(df.date).dt.strftime('%d/%m/%Y')
    df['id'] = df.id.astype(str)
    df['title'] = df.title.str.strip().apply(
        lambda x: np.nan if re.search(r'^\s*$', x) else x
    )
    df['title_low'] = df.title.str.lower()
    df = df.groupby('title', as_index=False).first()
    df.dropna(
        subset=['title'],
        inplace=True
    )

    return df


def clean_clinicaltrial_df(file_path: str) -> pd.DataFrame:
    """clean clinical_trial.parquet"""

    df = pd.read_parquet(file_path)
    df.rename(
        columns={'scientific_title': 'title'},
        inplace=True
    )
    df['date'] = pd.to_datetime(df.date).dt.strftime('%d/%m/%Y')
    df['title'] = df.title.str.strip().apply(
        lambda x: np.nan if re.search(r'^\s*$', x) else x
    )
    df['title_low'] = df.title.str.lower()
    df = df.groupby('title', as_index=False).first()
    df.dropna(
        subset=['title'],
        inplace=True
    )
    
    return df


def concat_pubmed(csv_path: str, json_path:str) -> pd.DataFrame:
    """
    Concatenate 2 sources (csv & json) into on single dataframe
    """

    csv_pubmed = pd.read_parquet(csv_path)
    json_pubmed = pd.read_parquet(json_path)
    csv_pubmed['id'] = csv_pubmed.id.astype(str)
    json_pubmed['id'] = json_pubmed.id.astype(str)
    df = pd.concat(
        [csv_pubmed, json_pubmed],
        ignore_index=True
    )

    return df


def find_drug_in_title(row, searched_drugs: list) -> list:
    """
    Find drugs in the title of medical trials or publications
    """

    found_drugs = [drug for drug in searched_drugs if drug in row]

    return found_drugs


def prepare_pubmed(pubmed_path: str, drug_path: str) -> pd.DataFrame:
    """
    Prepare for exporting to json
    """

    pubmed = pd.read_parquet(pubmed_path)
    drug = pd.read_parquet(drug_path)

    # List of unique drugs we are searching
    searched_drugs = list(drug.drug.unique())

    pubmed['drug'] = pubmed.title_low.apply(
        find_drug_in_title, 
        args=(searched_drugs,)
    )
    pubmed = pubmed.explode('drug')

    pubmed['pubmeds'] = pubmed.apply(
        lambda x: {'title': x.title, 'date': x.date} if not pd.isna(x.title) else np.nan, 
        axis=1
    )

    return pubmed


def prepare_clinicaltrial(trial_path: str, drug_path: str) -> pd.DataFrame:
    """
    Prepare for exporting to json
    """

    trial = pd.read_parquet(trial_path)
    drug = pd.read_parquet(drug_path)

    # List of unique drugs we are searching
    searched_drugs = list(drug.drug.unique())

    trial['drug'] = trial.title_low.apply(
        find_drug_in_title, 
        args=(searched_drugs,)
    )
    trial = trial.explode('drug')

    trial['trials'] = trial.apply(
        lambda x: {'title': x.title, 'date': x.date} if not pd.isna(x.title) else np.nan, 
        axis=1
    )

    return trial


def prepare_journal(pubmed_file: str, clinicaltrial_file: str) -> pd.DataFrame:
    """
    Prepare for exporting to json
    """

    pubmeds_by_drug = pd.read_parquet(pubmed_file)
    trials_by_drug = pd.read_parquet(clinicaltrial_file)
    journals = pd.concat([pubmeds_by_drug, trials_by_drug])
    journals = journals[['drug', 'journal', 'date']].drop_duplicates()
    journals['journals'] = journals.apply(
        lambda x: {'name': x.journal, 'date': x.date} if not pd.isna(x.journal) else np.nan, 
        axis=1
    )

    return journals


def merge_data(drugs_file: str, pubmed_file: str, clinicaltrial_file: str) -> pd.DataFrame:
    """merge data and return the final json"""

    drugs = pd.read_parquet(drugs_file)
    pubmed = pd.read_parquet(pubmed_file)
    clinicaltrial = pd.read_parquet(clinicaltrial_file)

    # List of unique drugs we are searching
    searched_drugs = list(drugs.drug.unique())

    pubmed['drug'] = pubmed.title_low.apply(
        find_drug_in_title, 
        args=(searched_drugs,)
    )
    pubmed = pubmed.explode('drug')
    clinicaltrial['drug'] = clinicaltrial.title_low.apply(
        find_drug_in_title, 
        args=(searched_drugs,)
    )
    clinicaltrial = clinicaltrial.explode('drug')

    # Merge
    drug_clinicaltrial = drugs.merge(
        clinicaltrial,
        how='inner',
        on=['drug']
    )
    drug_pubmed = drugs.merge(
        pubmed,
        how='inner',
        on=['drug']
    )
    output = drug_clinicaltrial.merge(
        drug_pubmed,
        how='outer',
        on='drug',
        suffixes=('_pubmed', '_trial')
    )

    return output
