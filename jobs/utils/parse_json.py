import json

from jsonschema import Draft6Validator, validate


def check_schema(schema):
    return Draft6Validator.check_schema(schema)


def check_data(data, schema):
    validate(instance=data, schema=schema)


def extract_from_schema(data, schema):
    result = list()
    for name, data_value in schema["properties"].items():
        reference = data_value.get("$ref", False)
        if reference:
            data_value = _get_reference(reference, schema)
        if data_value["type"] == 'object':
            additional_result = extract_from_schema(data[name], data_value)
            result = result + additional_result
            continue
        required = name in schema.get("required", [])
        result.append({
            "key": name,
            "value": data[name] if required else data.get(name, None),  # add types number, string, integer
            "required": required
        })
    return result


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


def json_extract_with_schema(df, column_name, json_schema, overwrite=False):
    columns = (list() if overwrite else df.columns) + list(_extract_columns_from_schema(json_schema))

    extraction = df.rdd \
        .map(lambda x: (tuple() if overwrite else x) + _extract_value_with_schema(x, column_name, json_schema)) \
        .toDF(columns)
    return extraction


def _extract_columns_from_schema(json_schema):
    result = list()
    for key, value in json_schema["properties"].items():
        if value.get("$ref", False):
            value = _get_reference(value.get('$ref'), json_schema)
        value_type = value["type"]
        if value_type != 'object':
            result.append(key)
        elif value_type == 'object':
            result = result + _extract_columns_from_schema(value)
        else:
            raise ValueError(f'type not defined for {key}: {value}')
    return result


def _extract_value_with_schema(x, column_name, json_schema):
    json_data = json.loads(x[column_name])
    if not isinstance(json_data, dict):
        raise TypeError(f"""{x[column_name]} in column {column_name} has not a json structure""")

    data = extract_from_schema(json_data, json_schema)
    result = list()
    for value in data:
        result.append(value["value"])
    return tuple(result)
