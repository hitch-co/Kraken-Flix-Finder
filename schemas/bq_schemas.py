from google.cloud import bigquery

def get_bq_schemas():
    user_login_schema = [
        bigquery.SchemaField("username", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("password", "STRING"),
        bigquery.SchemaField("date_created", "DATETIME"),
        bigquery.SchemaField("is_active", "BOOLEAN"),
        bigquery.SchemaField("is_admin", "BOOLEAN"),
    ]

    #Schema dictionary for each known budget file
    bq_schemas = {
        'user_login_schema': user_login_schema,
    }

    #return schemas in dictionary
    return bq_schemas

if __name__ == "__main__":
    bq_schemas = get_bq_schemas()
    print(bq_schemas['schema_historic_weather'])

    bq_forecast_schema = bq_schemas['schema_historic_forecast']
    forecast_field_names = [field.name for field in bq_forecast_schema]
    forecast_arrays = {field.name: [] for field in bq_forecast_schema}
    forecast_arrays['capture_date']
    print(forecast_field_names)
    print(forecast_arrays)