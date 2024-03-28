import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
import azure.functions as func
import datetime
import logging

ingest_to_kusto = True #ingests to kusto if true, prints kusto query if false
force_ingestion = False #forces ingestion to kusto (even if nothing changed) if true, skips ingestion when there are no changes if false

def main(mytimer: func.TimerRequest) -> None:
  # add azure function appid as db admin to make managedidentity work
  
  cluster = "https://azsphere.eastus.kusto.windows.net"
  kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster)
  client = KustoClient(kcsb)
  ingested_row_count = 0
  ENVIRONMENTS = ["prod", "preprod"]
  db = "DeviceInsightsProd"
  tablename = "OsVersionImages"
  json_mappings = []
  session = requests.Session()
  retry = Retry(connect=10, backoff_factor=0.25)
  adapter = HTTPAdapter(max_retries=retry)
  session.mount('http://', adapter)
  session.mount('https://', adapter)

  for environment in ENVIRONMENTS:
    jsondata=session.get("https://" + environment + ".releases.sphere.azure.net/versions/mt3620an.json").json()
    osversions = lower_key(session.get("https://" + environment + ".releases.sphere.azure.net/versions/mt3620an.json").json())["versions"]
    for osversion in osversions:
      name = osversion["name"]
      for image in osversion["images"]:
        cid = image["cid"]
        iid = image["iid"]
        curMapping = {
          "OsVersion": name,
          "ComponentId": cid,
          "ImageId": iid,
          "Environment": environment
        }
        json_mappings.append(curMapping)

  readquery = tablename
  response = client.execute(db, readquery) #notify on exception here
  kusto_mappings = response.primary_results[0].to_dict()["data"]
  new_rows, shouldReplace = getMappingDiff(json_mappings, kusto_mappings)
  command = ".set-or-replace " if shouldReplace else ".set-or-append "
  query = command + tablename + " <|\ndatatable (OsVersion:string, Environment:string, ComponentId:string, ImageId:string) [\n"
  for row in new_rows:
    query += "\"" + row["OsVersion"] + "\",\"" + row["Environment"] + "\",\"" + row["ComponentId"] + "\",\"" + row["ImageId"] + "\",\n"
  query += "]"
  logging.info("Json query formed:" +  str(datetime.datetime.now()) + str(query) )

  if ingest_to_kusto:
    if not new_rows:
      logging.info("Mappings haven't changed. Skipping Kusto ingestion")
    else:
      logging.info("Mappings have changed, Ingesting to Kusto.\n" + query)
      try:
        response = client.execute(db, query) #notify on exception here
        ingested_row_count = response.primary_results[0][0]["RowCount"]
      except:
        logging.error("Ingesting to Kusto Failed at" + str(datetime.datetime.now()) )

      if len(new_rows) != ingested_row_count:
        logging.error("Unexpected ingestion size. Expected: "+  str(len(new_rows))+ "; Ingested: " + str(ingested_row_count))
      else:
        logging.info("Kusto ingestion successful, Ingested row count" +  str(ingested_row_count))
  else:
    logging.info("No Ingestion\n" + str(query))

# returns tuple of (rows to ingest, should replace table)
def getMappingDiff(json_file_mappings, kusto_mappings):
  if force_ingestion:
    # ingest everything and replace table
    return (json_file_mappings, True)
  new_in_json = [x for x in json_file_mappings if x not in kusto_mappings]
  new_in_kusto = [x for x in kusto_mappings if x not in json_file_mappings]
  if not new_in_kusto:
    # ingest new rows only and don't replace table
    return (new_in_json, False)
  else:
    # existing mapping changed. ingest everything and replace table
    return (json_file_mappings, True)

#convert lower key in case of json contains upper keys
def lower_key(in_dict):
  if type(in_dict) is dict:
      out_dict = {}
      for key, item in in_dict.items():
          out_dict[key.lower()] = lower_key(item)
      return out_dict
  elif type(in_dict) is list:
      return [lower_key(obj) for obj in in_dict]
  else:
      return in_dict