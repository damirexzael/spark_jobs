import json


def _extract_value(x, column_name, json_schema):
    json_data = json.loads(x[column_name])
    if not isinstance(json_data, dict):
        raise TypeError(f"""{x[column_name]} in column {column_name} has not a json structure""")

    return_values = list()
    for key, value in json_schema.items():
        extracted_value = json_data
        try:
            for path in value:
                extracted_value = extracted_value[path]
            extracted_value = str(extracted_value)
            return_values.append(extracted_value)
        except KeyError as e:
            raise KeyError(f"""key {e} not found, searching {value} in {json.dumps(json_data)}""")
    return tuple(return_values)


def json_extract(df, column_name, json_schema, overwrite=False):
    columns = (list() if overwrite else df.columns) + list(json_schema.keys())

    extraction = df.rdd \
        .map(lambda x: (tuple() if overwrite else x) + _extract_value(x, column_name, json_schema)) \
        .toDF(columns)
    return extraction
