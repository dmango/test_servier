import pandas as pd
import json


def save_to_local(pubmed_path: str, clinical_trial_path: str, journal_path: str, file_path='output.json'):
    """
    Prepare data and export to json
    """

    pubmed = pd.read_parquet(pubmed_path)
    clinical_trial = pd.read_parquet(clinical_trial_path)
    journal = pd.read_parquet(journal_path)

    list_pubmeds_by_drug = pd.DataFrame(
        pubmed.groupby('drug')['pubmeds'].apply(
            lambda x: x.tolist() if not all(pd.isna(x)) else []
        )
    )
    list_trials_by_drug = pd.DataFrame(
        clinical_trial.groupby('drug')['trials'].apply(
            lambda x: x.tolist() if not all(pd.isna(x)) else []
        )
    )
    list_journals_by_drug = pd.DataFrame(
        journal.groupby('drug')['journals'].apply(
            lambda x: x.tolist() if not all(pd.isna(x)) else []
        )
    )

    output_df = pd.concat(
        [
            list_journals_by_drug, 
            list_trials_by_drug, 
            list_pubmeds_by_drug
        ], 
        axis=1
    )
    output_df['trials'] = output_df['trials'].apply(lambda d: d if isinstance(d, list) else [])
    output_df['pubmeds'] = output_df['pubmeds'].apply(lambda d: d if isinstance(d, list) else [])
    output_df['journals'] = output_df['journals'].apply(lambda d: d if isinstance(d, list) else [])

    result = output_df.to_json(orient='index')
    parsed = json.loads(result)
    with open(file_path, 'w') as f:
        json.dump(
            parsed, 
            fp=f, 
            indent=4
        )
