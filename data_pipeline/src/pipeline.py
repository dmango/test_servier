from pkg_resources import resource_filename

# from etl import extract
from data_pipeline.etl import extract, transform, load


if __name__ == "__main__":

    drugs_path = resource_filename('data_pipeline', 'resources/drugs.csv')
    pubmed_csv_path = resource_filename('data_pipeline', 'resources/pubmed.csv')
    pubmed_json_path = resource_filename('data_pipeline', 'resources/pubmed.json')
    clinical_trials_path = resource_filename('data_pipeline', 'resources/clinical_trials.csv')

    #STEP 1: Extraction
    #data extraction
    drugs_df = extract.extract_from_local(drugs_path)
    clinicaltrials_df = extract.extract_from_local(clinical_trials_path)
    pubmed_csv_df = extract.extract_from_local(pubmed_csv_path)
    pubmed_json_df = extract.extract_from_local(pubmed_json_path)
    #save as parquet files
    drugs_df.to_parquet("raw_drugs.parquet")
    pubmed_csv_df.to_parquet("raw_pubmed_csv.parquet")
    pubmed_json_df.to_parquet("raw_pubmed_json.parquet")
    clinicaltrials_df.to_parquet("raw_trials.parquet")

    #STEP 2: Transformation
    #data cleaning, transformation
    pubmed_df = transform.concat_pubmed("raw_pubmed_csv.parquet", "raw_pubmed_json.parquet")
    drugs_df = transform.clean_drugs_df("drugs_raw.parquet")
    pubmed_df = transform.clean_pubmed_df("pubmed_raw.parquet")
    clinicaltrials_df = transform.clean_clinicaltrial_df("clinicaltrials_raw.parquet")

    #save as parquet file
    drugs_df.to_parquet("drugs_clean.parquet")
    pubmed_df.to_parquet("pubmed_clean.parquet")
    clinicaltrials_df.to_parquet("clinicaltrials_clean.parquet")

    #Preparation
    pubmed_df = transform.prepare_pubmed("pubmed_clean.parquet", "drugs_clean.parquet")
    clinicaltrials_df = transform.prepare_clinicaltrial("clinicaltrials_clean.parquet", "drugs_clean.parquet")
    pubmed_df.to_parquet("pubmed_prepare.parquet")
    clinicaltrials_df.to_parquet("clinicaltrials_prepare.parquet")
    journals_df = transform.prepare_journal("pubmed_prepare.parquet", "clinicaltrials_prepare.parquet")
    journals_df.to_parquet("journals_prepare.parquet")

    #STEP 3: Load
    load.save_to_local('pubmed_prepare.parquet', 'clinicaltrials_prepare.parquet', 'journals_prepare.parquet')

