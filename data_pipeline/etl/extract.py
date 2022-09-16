import pandas as pd
import json5


def read_csv(file_path: str) -> pd.DataFrame:
    """
    Read from csv and put into a Pandas dataframe
    """

    df = pd.read_csv(file_path)

    return df


def read_json(file_path: str) -> pd.DataFrame:
    """
    Read from json and put into a Pandas dataframe
    """

    with open(file_path, 'r') as f:
        json_str = f.read()
    # Convert JSON string to a list of records
    json_list = json5.loads(json_str)
    df = pd.DataFrame.from_dict(json_list)
    df['id'] = df.id.astype(str)

    return df


def extract_from_local(file_path: str) -> pd.DataFrame:
    """"
    Extract from file located in the local file system
    """

    format = file_path.split('.')[-1]
    try:
        df = None
        if format == "csv":
            df = read_csv(file_path)
        elif(format == "json"):
            df = read_json(file_path)
    except:
        pass

    return df