import json
from collections import defaultdict

def get_journal_w_max_drugs(json_file_path:str) -> str:
    json_file = None
    with open(json_file_path, 'r') as f:
        json_file = json.loads(f.read())

    journal_dict = defaultdict(list)
    #get the list of drugs mentioned in journals
    for drug in json_file:
        for journal in json_file[drug]["journals"]:
            journal_name = journal["name"]
            journal_dict[journal_name].append(drug)
    #sort (ascending) the journal by number of different drugs
    s = sorted(journal_dict.items(), key=lambda x: len(set(x[1])), reverse=True)
    #get the first element: journal with the max number of drugs mention
    return s[0][0]