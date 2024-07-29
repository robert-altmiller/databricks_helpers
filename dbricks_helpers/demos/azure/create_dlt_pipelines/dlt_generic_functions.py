def generate_dynamic_sql(table_name = None, table_ref = None, select_columns = None, group_by_columns = None, where_clauses = None):
    """generates a dynamic sql statement"""
    if select_columns is None: select_statement = "*"
    else: 
        select_statement = ", ".join([f"<table_ref>.<{col}> AS {col}" if "(" not in col else f"{col}" for col in select_columns])
        select_statement = select_statement.rstrip(",")

    if group_by_columns != None: 
        group_by_statement = ", ".join([f"<table_ref>.<{col}>" for col in group_by_columns])
        sql = f"SELECT {select_statement} FROM <table_name> AS <table_ref> GROUP BY {group_by_statement}"
    else: sql = f"SELECT {select_statement} FROM <table_name> AS <table_ref>"

    if where_clauses is not None:
        where_statement = " ".join([f"<table_ref>.<{clause.split(' ')[0]}> {clause.split(' ', 1)[1]}" if "col" in clause else clause for clause in where_clauses])
        sql += f" WHERE {where_statement}"

    return sql


def create_dynamic_sql_dict(table_name = None, table_ref = None, select_columns = None):
    """create dynamic sql column lookup dictionary"""
    dict = {}
    for col in select_columns:
        dict[col] = col
    dict["table_name"] = table_name
    dict["table_ref"] = table_ref
    return dict
        

def insert_col_mappings_into_sql_template(col_mapping = None, sql_template = None):
    """insert column mappings into sql template"""
    for key, val in col_mapping.items():
        sql_template = sql_template.replace(f"<{key}>", val)    
    return sql_template

# # Example usage 1
# table_name = "<table_name>"
# table_ref = "a"
# select_columns = ["col1", "col2", "col3", "COUNT(*)"]
# group_by_columns = ["col1", "col2", "col3"]
# where_clauses = ["col1 > 0 AND", "col2 != Null OR", "col3 < 100"]
# sql_query = generate_dynamic_sql(table_name, table_ref, select_columns, group_by_columns, where_clauses)
# print(f"example1: {sql_query")

# # Example usage 2
# table_name = "<table_name>"
# table_ref = "a"
# select_columns = ["col1", "col2", "col3"]
# sql_query = generate_dynamic_sql(table_name, table_ref, select_columns, None, None)
# print(f"example2: {sql_query")

# # Example usage 3
# table_name = "<table_name>"
# table_ref = "a"
# select_columns = ["col1", "col2", "col3"]
# where_clauses = ["col1 > 0 AND", "col2 != Null OR", "col3 < 100"]
# sql_query = generate_dynamic_sql(table_name, table_ref, select_columns, None, where_clauses)
# print(f"example3: {sql_query")

# # Example usage 4
# table_name = "<table_name>"
# table_ref = "a"
# select_columns = ["col1", "col2", "col3"]
# group_by_columns = ["col1", "col2", "col3"]
# sql_query = generate_dynamic_sql(table_name,table_ref, select_columns, group_by_columns, None)
# print(f"example4: {sql_query")

# # Example usage 5
# table_name = "<table_name>"
# table_ref = "a"
# sql_query = generate_dynamic_sql(table_name, table_ref, None, None, None)
# print(f"example5: {sql_query")
