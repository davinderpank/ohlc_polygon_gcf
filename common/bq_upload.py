from google.cloud import bigquery


def df_to_bqupload(project, table_name, df):
    client = bigquery.Client(project=project)
    client.load_table_from_dataframe(df, table_name).result()