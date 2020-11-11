import io

table_columns_query = """
SELECT column_name
FROM {db_name}.information_schema.columns
WHERE table_schema ilike '{schema}'
  AND table_name ilike '{table}'
"""

distinct_values_query = """
SELECT DISTINCT {columns} {types}
FROM "{schema}"."{table}"
"""

# dictionary to rename data vault primary keys into the expected names for business vault primary keys
pk_dictionary = {
    'MD5_HUB_AE_LEASE': 'LEASE_AE_KEY',
    'MD5_HUB_AE_LOAN': 'LOAN_AE_KEY',
    'MD5_HUB_AE_REVEX': 'REVEX_AE_KEY',
    'MD5_HUB_AT_SCENARIO': 'SCENARIO_AT_KEY',
    'MD5_HUB_AE_PROPERTY': 'PROPERTY_AE_KEY',
    'MD5_HUB_AT_ENTITY': 'ENTITY_AT_KEY'
}


def get_columns(cursor, database, schema, table):
    """
    Return a list of columns in the specified table.
    :param cursor: database cursor
    :param database: database name
    :param schema: database schema
    :param table: table name
    :return: list of columns in the specified table
    """
    query = table_columns_query.format(
        db_name=database,
        table=table,
        schema=schema
    )

    cursor.execute(query)
    rows = list(cursor)

    if not rows:
        raise Exception('Table {}.{} empty or not found'.format(
            schema, table
        ))

    return [row[0] for row in rows]


def get_distinct_values(cursor, schema, table, columns, type_column=None):
    """
    Returns the distinct values in specified columns of a table
    :param cursor: database cursor
    :param schema: database schema
    :param table: table name
    :param columns: list of columns
    :param type_column: optional column containing data type information
    :return: list of distinct values in the specified columns
    """
    columns = ', '.join('"{}"'.format(col) for col in columns)
    if type_column:
        types = ', ' + type_column[0]
    else:
        types = ''
    query = distinct_values_query.format(
        schema=schema,
        table=table,
        columns=columns,
        types=types
    )
    cursor.execute(query)
    return list(cursor)


def _generate_pivot_query(cursor, source_database=None, source_schema=None, source_table=None, base_columns=None,
                          pivot_columns=None, exclude_columns=None, type_column=None, **kwargs):
    """
    Generates the SQL query to pivot data
    :param cursor: database cursor
    :param source_database: database name
    :param source_schema: database schema
    :param source_table: table name
    :param base_columns: base columns to pivot data
    :param pivot_columns: columns with data to aggregate
    :param exclude_columns: columns to be excluded from results
    :param type_column: column with data type information
    :param kwargs:
    :return: an SQL query string
    """
    if not source_database:
        raise Exception('No source_database provided')
    if not source_schema:
        raise Exception('No source_schema provided')
    if not source_table:
        raise Exception('No source_table provided')
    if not base_columns:
        raise Exception('No base_columns provided')
    if not pivot_columns:
        raise Exception('No pivot_columns provided')

    out = io.StringIO()

    exclude_columns = exclude_columns or []
    type_column = type_column or []

    # Convert strings of comma-separated column names into lists
    if isinstance(base_columns, str):
        base_columns = base_columns.split(',')
    elif isinstance(base_columns, list) and len(base_columns) == 1:
        base_columns = base_columns[0].split(',')

    if isinstance(pivot_columns, str):
        pivot_columns = pivot_columns.split(',')
    elif isinstance(pivot_columns, list) and len(pivot_columns) == 1:
        pivot_columns = pivot_columns[0].split(',')

    if isinstance(exclude_columns, str):
        exclude_columns = exclude_columns.split(',')
    elif isinstance(exclude_columns, list) and len(exclude_columns) == 1:
        exclude_columns = exclude_columns[0].split(',')

    if isinstance(type_column, str):
        type_column = type_column.split(',')
    elif isinstance(type_column, list) and len(type_column) == 1:
        type_column = type_column[0].split(',')
    if len(type_column) > 1:
        raise Exception('There is more than one type column: {}'.format(type_column))

    # Check there is no intersection among the lists
    checklist1 = list(set(base_columns) & set(pivot_columns))
    if len(checklist1) > 0:
        raise Exception('Base and pivot lists share some columns: {}'.format(checklist1))
    checklist2 = list(set(base_columns) & set(exclude_columns))
    if len(checklist2) > 0:
        raise Exception('Base and exclude lists share some columns: {}'.format(checklist2))
    checklist3 = list(set(base_columns) & set(type_column))
    if len(checklist3) > 0:
        raise Exception('Base and type lists share some columns: {}'.format(checklist3))
    checklist4 = list(set(pivot_columns) & set(exclude_columns))
    if len(checklist4) > 0:
        raise Exception('Pivot and exclude lists share some columns: {}'.format(checklist4))
    checklist5 = list(set(pivot_columns) & set(type_column))
    if len(checklist5) > 0:
        raise Exception('Pivot and type lists share some columns: {}'.format(checklist5))
    checklist6 = list(set(exclude_columns) & set(type_column))
    if len(checklist6) > 0:
        raise Exception('Exclude and type lists share some columns: {}'.format(checklist6))

    table_columns = get_columns(cursor, source_database, source_schema, source_table)

    inhibit_pivot = base_columns + pivot_columns + exclude_columns + type_column

    columns_to_pivot = [c for c in table_columns if c not in inhibit_pivot]

    # Get distinct values for the base columns
    distinct_values = get_distinct_values(cursor, source_schema, source_table, pivot_columns, type_column)

    out.write('SELECT\n')
    # Write base columns
    for i, col in enumerate(base_columns):
        out.write('    ')
        if i > 0:
            out.write(', ')
        # replace PK keys names
        if col in pk_dictionary:
            out.write(col + ' AS "' + pk_dictionary[col] + '"\n')
        else:
            out.write(col + '\n')
    if len(distinct_values) > 0:
        # Write columns to pivot
        for col in columns_to_pivot:
            pivoted = pivot_column(col, pivot_columns, distinct_values, len(columns_to_pivot), type_column)
            pivoted.sort()
            for col_ in pivoted:
                out.write('    , {}\n'.format(col_))
    # Write from clause
    out.write('FROM\n    {}.{}\n'.format(source_schema, source_table))
    # Write grouping clause
    out.write('GROUP BY\n')
    for i, col in enumerate(base_columns):
        out.write('    ')
        if i > 0:
            out.write(', ')
        out.write(col + '\n')

    return out.getvalue()


def pivot_column(column, pivot_columns, distinct_values, number_of_columns_to_pivot, type_column, **kwargs):
    default = 'NULL'

    # Make sub-condition strings and names
    tests = []
    names = []
    for row in distinct_values:
        test = []
        name = []
        for col, val in zip(pivot_columns, row):
            if val is None:
                test.append('{} is NULL'.format(col))
                name.append('{}_null'.format(col))
            else:
                # name_suffix = re.sub('[^0-9A-Za-z_]+', '_', str(val))
                if isinstance(val, (int, float)):
                    val_str = str(val)
                else:
                    val_str = "'{}'".format(val)
                test.append('{} = {}'.format(col, val_str))

                if number_of_columns_to_pivot > 1:
                    name.append('{}_{}'.format(val, col))
                else:
                    name.append('{}'.format(val))
        tests.append(' AND '.join(test))
        names.append('_'.join(name))

    # build a type dictionary if there is a type column
    types = {}
    if len(distinct_values[0]) == 2:
        for row in distinct_values:
            types[row[0]] = row[1]

    result = []
    # TODO
    # Currently using MAX to aggregate, planning to extend functionality based on data type, i.e. SUM for numeric value
    for test, suffix in zip(tests, names):
        if number_of_columns_to_pivot > 1:
            column_name = '{suffix}_{column}'.format(column=column, suffix=suffix)
        else:
            column_name = '{suffix}'.format(suffix=suffix)

        if len(types) > 0:
            if types[suffix].upper() == 'TEXT':
                # cast the value to string (varchar)
                formula = "MAX(CASE WHEN {test} AND {type_col} = '{type}' " \
                          "THEN TO_VARCHAR({column}) ELSE {default} END) AS \"{column_name}\"".format(
                    test=test,
                    type_col=type_column[0],
                    type=types[suffix],
                    column=column,
                    default=default,
                    column_name=column_name
                )
            elif types[suffix].upper() == 'NUMERIC':
                # cast the value to decimal(17,4), the default numeric precision for Argus Taliance
                formula = "MAX(CASE WHEN {test} AND {type_col} = '{type}' " \
                          "THEN TRY_TO_NUMERIC({column}, 17, 4) ELSE {default} END) AS \"{column_name}\"".format(
                    test=test,
                    type_col=type_column[0],
                    type=types[suffix],
                    column=column,
                    default=default,
                    column_name=column_name
                )
            elif types[suffix].upper() == 'FLOAT' or types[suffix].upper() == 'DOUBLE':
                # cast the value to double, the default numeric precision for Argus Enterprise
                formula = "MAX(CASE WHEN {test} AND {type_col} = '{type}' " \
                          "THEN TRY_TO_DOUBLE({column}) ELSE {default} END) AS \"{column_name}\"".format(
                    test=test,
                    type_col=type_column[0],
                    type=types[suffix],
                    column=column,
                    default=default,
                    column_name=column_name
                )
            elif types[suffix].upper() in ('BOOLEAN', 'BOOL', 'TRUE/FALSE'):
                # cast the value to boolean
                formula = "MAX(CASE WHEN {test} AND {type_col} = '{type}' " \
                          "THEN TRY_TO_BOOLEAN({column}) ELSE {default} END) AS \"{column_name}\"".format(
                    test=test,
                    type_col=type_column[0],
                    type=types[suffix],
                    column=column,
                    default=default,
                    column_name=column_name
                )
            elif types[suffix].upper() == 'INTEGER':
                # cast the value to number(38,0)
                formula = "MAX(CASE WHEN {test} AND {type_col} = '{type}' " \
                          "THEN TRY_TO_NUMBER({column}) ELSE {default} END) AS \"{column_name}\"".format(
                    test=test,
                    type_col=type_column[0],
                    type=types[suffix],
                    column=column,
                    default=default,
                    column_name=column_name
                )
            elif types[suffix].upper() == 'DATE':
                # cast the value to date with default format
                formula = \
                    "MAX(CASE WHEN {test} AND {type_col} = '{type}' THEN TRY_TO_DATE({column}) ELSE {default} END) AS \"{column_name}\"".format(
                        test=test,
                        type_col=type_column[0],
                        type=types[suffix],
                        column=column,
                        default=default,
                        column_name=column_name
                    )
            # TODO add additional data types as needed
            else:
                # unrecognised data type, pass the value as it is
                formula = "MAX(CASE WHEN {test} AND {type_col} = '{type}' " \
                          "THEN {column} ELSE {default} END) AS \"{column_name}\"".format(
                    test=test,
                    type_col=type_column[0],
                    type=types[suffix],
                    column=column,
                    default=default,
                    column_name=column_name
                )
        else:
            formula = 'MAX(CASE WHEN {test} THEN {column} ELSE {default} END) AS "{column_name}"'.format(
                test=test,
                column=column,
                default=default,
                column_name=column_name
            )

        result.append(formula)

    return result
