import json

from jsonschema import Draft6Validator, validate


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


def check_schema(schema):
    return Draft6Validator.check_schema(schema)


def check_data(data, schema):
    validate(instance=data, schema=schema)


def extract_from_schema(data, schema):
    values = list()
    required = schema.get('required', [])
    for key, value in schema["properties"].items():
        data_output = data[key] if key in required else data.get(key, None)
        if data_output:
            data_output = _cast_value(data_output, value, schema)
        values.append({
            "key": key,
            "value": data_output,
            "required": key in required
        })
    return values


def _cast_value(data_output, value, schema):
    if value.get('$ref', False):
        value = _get_reference(value.get('$ref'), schema)

    if value.get('type') == 'number':
        parser = float
    elif value.get('type') == 'string':
        parser = str
    else:
        raise TypeError(f"Not find {value} json type")
    return parser(data_output)


def _get_reference(reference, schema):
    for key in reference.split('/')[1:]:
        schema = schema[key]
    return schema
