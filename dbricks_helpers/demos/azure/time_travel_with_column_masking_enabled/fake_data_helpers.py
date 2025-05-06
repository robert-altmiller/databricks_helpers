# Databricks notebook source
# DBTITLE 1,Insert Fake PII Data into UC Delta Table
def insert_pii_data(table_fqn: str, records_to_insert: int) -> None:
    """
    Inserts a specified number of fake PII records into a Unity Catalog table.

    Args:
        table_fqn (str): Fully qualified name of the target table (e.g., 'catalog.schema.table').
        records_to_insert (int): Number of fake records to generate and insert.

    Returns:
        None
    """
    # Initialize the Faker generator for generating mock PII data
    fake = Faker()

    # Generate a list of Row objects with synthetic PII data
    data = [Row(
        first_name=fake.first_name(),
        last_name=fake.last_name(),
        email=fake.email(),
        phone=fake.phone_number(),
        ssn=fake.ssn(),
        dob=fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
        address=fake.address()
    ) for _ in range(records_to_insert)]

    # Create a Spark DataFrame from the fake data
    df = spark.createDataFrame(data)

    # Append the DataFrame to the target table
    df.write.mode("append").saveAsTable(table_fqn)

    # Log how many records were inserted and the current total in the table
    print(f"appended {records_to_insert} records to table {table_fqn}")
    print(f"total records in table {table_fqn}: {spark.table(table_fqn).count()}")
