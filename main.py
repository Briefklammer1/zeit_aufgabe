def logs_parser(data, context):
    print("works finally")
    from google.cloud import bigquery
    from google.cloud import storage
    import pandas as pd
    from apachelogs import LogParser
    
    if not data["name"].endswith(".log"):
        return "--> wrong datatype to process..."

    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    dataset_id = 'dataset_zeit'
    table_id = 'logsF'
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    table = bigquery_client.get_table(table_ref)

    bucket = storage_client.get_bucket(data['bucket'])
    blob = bucket.blob(data['name'])

    logs = blob.download_as_string()[:-1].decode().split('\n')

    log_format = '%h \"%l\" \"%u\" %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"'
    log_keys = ["%h", "%l", "%u", "%t", "%r", "%>s", "%b", "%{Referer}i", "%{User-Agent}i"]

    log_parser = LogParser(log_format)

    pdf = pd.DataFrame(columns=log_keys)
    for key in log_keys:
        pdf[key] = [row.directives[key] for row in log_parser.parse_lines(logs)]
    
    del pdf["%h"]
    del pdf["%l"]
    del pdf["%u"]
    pdf.rename(columns={'%>s': 'status', '%b':'bytesize', '%{Referer}i':'referer',
                        '%r':'request', '%{User-Agent}i': 'useragent', '%t': 'time'}, inplace=True)

    bigquery_client.insert_rows_from_dataframe(table, pdf)

    return "--> Load process finished."
