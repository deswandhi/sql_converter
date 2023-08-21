import sqlglot
import yaml
import os
from google.cloud import storage
from pathlib import Path


bucket_name = "dataform-dev-poc-ioh-deloitte"
prefix = 'DS_Conversion'
dl_dir = 'gcsfile'
config = yaml.load(open('config.yaml'), Loader=yaml.FullLoader)
list_scenario = config["scenario"]
source_dialect = config["source_dialect"]
target_dialect = config["target_dialect"]


print(f"Source Dialect: {source_dialect}, Target Dialect: {target_dialect}")
print("List Scenario")
for sc in list_scenario:
    print(f"- {sc} ")


gcs = storage.Client()

bucket = gcs.get_bucket(bucket_name)
seq_file = bucket.blob(f"{prefix}/Sequence.csv")
seq_file.download_to_filename(f"{dl_dir}/Sequence.csv")

def dL_scenario_sql(scenario, src_sql):
    blobs = bucket.list_blobs(prefix=f"{prefix}/{scenario}/")

    for blob in blobs:
        sce_name = blob.name.split("/")[-2]
        filename = blob.name.split("/")[-1]
        try:
            os.makedirs(f"{dl_dir}/{src_sql}/{sce_name}")
        except:
            pass
        blob.download_to_filename(f"{dl_dir}/{src_sql}/{sce_name}/{filename}")
        print(f"> Success download file : {dl_dir}/{src_sql}/{sce_name}/{filename}")

def translate_sql(scenario, src_sql, dest_sql):
    list_file = os.listdir(f"{dl_dir}/{src_sql}/{scenario}")
    for file in list_file:
        path = f"{dl_dir}/{src_sql}/{scenario}/{file}"
        file_path = Path(path)
        txt = file_path.read_text()
        result = sqlglot.transpile(txt, read=src_sql, write=dest_sql,identify=True, pretty=True)[0]
        path_result = f"translate/{target_dialect}/{scenario}"
        try:
            os.makedirs(path_result)
        except:
            pass
        with open (f"{path_result}/{file}", "w") as f:
            f.write(result)
            print(f">> Success translate to {target_dialect} dialect: {path_result}/{file}")
    return list_file

print("\n\t===========START===========\n")
for sc in list_scenario:
    dL_scenario_sql(sc, source_dialect)
    translate_sql(sc, source_dialect, target_dialect)
print("\n\t===========END===========")
