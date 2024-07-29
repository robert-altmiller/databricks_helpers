# library imports
from transformation_templates import *


# returns the correct column mapping dictionary
def get_col_mapping(func_name, tablename, tblref, catalog = None, schema = None, aggtype = None):
    func = globals().get(func_name)
    return func(tablename, tblref, catalog, schema, aggtype)


# share column mappings (silver/staging)
def get_col_mapping_shared():
    col_mapping_shared = {
        "msdyn_fueladdress": "''",
        "msdyn_fuelzip": "''",
        "msdyn_distance": "''",
        "msdyn_facilityId": "''",
        "msdyn_valuechainpartnerid": "''"
    }
    return col_mapping_shared


# column mappings for comdata (silver/staging)
def get_col_mapping_mc_comdata_bronze(tablename, tblref, catalog = None, schema = None, aggtype = None):
    col_mapping = {
        "tablename": tablename,
        "tblref": tblref,
        "msdyn_name": "'comdata'",
        "msdyn_origincorrelationid": f"concat('comdata','.',md5(concat_WS('.',f.msm_fuel_type_name, {tblref}.transaction_number, {tblref}.transaction_date)))",
        "msdyn_consumptionenddate": f"TO_DATE(TRIM({tblref}.transaction_date),'M/d/yyyy')",
        "msdyn_consumptionstartdate": f"TO_DATE(TRIM({tblref}.transaction_date),'M/d/yyyy')",
        "msdyn_dataqualitytype": "'actual'",
        "msdyn_organizationalunitid": "'medical-surgical solutions'",
        "msdyn_fueltypeid": "f.msm_fuel_type_name",
        "msdyn_quantity": f"CAST(COALESCE({tblref}.unit_gallons, 0) AS DECIMAL(10, 4))",
        "msdyn_quantityunit": "'gallons'",
        "transaction_id": f"{tblref}.transaction_number",
        "msdyn_vin": f"{tblref}.vin",
        "msdyn_cost": f"CAST(REPLACE({tblref}.ppu_ppg,'$','') AS DECIMAL(10,4))",
        "msdyn_costunit": "'usd'",
        "msdyn_evidence": "'invoice'",
        "msdyn_fuelquantity": f"CAST(REPLACE({tblref}.net_cost,'$','') AS DECIMAL(10,4))",
        "msdyn_fuelquantityunit": "'usd'",
        "msdyn_transactiondate": f"to_date({tblref}.transaction_date,'M/d/yyyy')",
        "msdyn_vehicletype": "'medium- and heavy-duty vehicles-diesel 2007-2018'",
        "load_timestamp": f"{tblref}.load_timestamp",
        "quarantined_flag_warn": "'valid_records'",
        "quarantined_flag_drop": "'valid_records'",
        "fuel_type": f"{tblref}.product_description" 
    }
    sql_template = ''.join([get_sql_silver_base_template(), get_sql_silver_base_template_addition1(catalog, schema).strip()])
    return {**col_mapping, **get_col_mapping_shared()}, sql_template


# column mappings for ryder (silver/staging)
def get_col_mapping_mc_penske_bronze(tablename, tblref, catalog = None, schema = None, aggtype = None):
    col_mapping = {
        "tablename": tablename,
        "tblref": tblref,
        "msdyn_name": "'penske'",
        "msdyn_origincorrelationid": f"concat('penske','.',md5(concat_WS('.',f.msm_fuel_type_name, {tblref}.fuel_ticket, {tblref}.invoice_date)))",
        "msdyn_consumptionenddate": f"TO_DATE(TRIM({tblref}.invoice_date),'M/d/yyyy')",
        "msdyn_consumptionstartdate": f"TO_DATE(TRIM({tblref}.invoice_date),'M/d/yyyy')",
        "msdyn_dataqualitytype": "'actual'",
        "msdyn_organizationalunitid": "'medical-surgical solutions'",
        "msdyn_fueltypeid": "f.msm_fuel_type_name",
        "msdyn_quantity": f"CAST(COALESCE({tblref}.gallons_pumped, 0)AS DECIMAL(10, 4))",
        "msdyn_quantityunit": "'gallons'",
        "transaction_id": f"{tblref}.fuel_ticket",
        "msdyn_vin": f"{tblref}.vin",
        "msdyn_cost": f"CAST(REPLACE({tblref}.price_per_gallon,'$','') AS DECIMAL(10,4))",
        "msdyn_costunit": "'usd'",
        "msdyn_evidence": "'invoice'",
        "msdyn_fuelquantity": f"CAST(REPLACE({tblref}.extended_price_per_gallon,'$','') AS DECIMAL(10,4))",
        "msdyn_fuelquantityunit": "'usd'",
        "msdyn_transactiondate": f"to_date({tblref}.invoice_date,'M/d/yyyy')",
        "msdyn_vehicletype": "'medium- and heavy-duty vehicles-diesel 2007-2018'",
        "load_timestamp": f"{tblref}.load_timestamp",
        "quarantined_flag_warn": "'valid_records'",
        "quarantined_flag_drop": "'valid_records'",
        "fuel_type": f"{tblref}.fuel_type" 
    }
    sql_template = ''.join([get_sql_silver_base_template(), get_sql_silver_base_template_addition1(catalog, schema).strip()])
    return {**col_mapping, **get_col_mapping_shared()}, sql_template


# column mappings for comdata (silver/staging)
def get_col_mapping_mc_ryder_bronze(tablename, tblref, catalog = None, schema = None, aggtype = None):
    col_mapping = {
        "tablename": tablename,
        "tblref": tblref,
        "msdyn_name": "'ryder'",
        "msdyn_origincorrelationid": f"concat('ryder','.',md5(concat_WS('.',f.msm_fuel_type_name, {tblref}.fuel_tkt_number, {tblref}.fuel_tkt_date)))",
        "msdyn_consumptionenddate": f"TO_DATE(TRIM({tblref}.fuel_tkt_date),'M/d/yyyy')",
        "msdyn_consumptionstartdate": f"TO_DATE(TRIM({tblref}.fuel_tkt_date),'M/d/yyyy')",
        "msdyn_dataqualitytype": "'actual'",
        "msdyn_organizationalunitid": "'medical-surgical solutions'",
        "msdyn_fueltypeid": "f.msm_fuel_type_name",
        "msdyn_quantity": f"CAST(COALESCE({tblref}.fuel_qty, 0)AS DECIMAL(10, 4))",
        "msdyn_quantityunit": "'gallons'",
        "transaction_id": f"{tblref}.fuel_tkt_number",
        "msdyn_vin": "NULL",
        "msdyn_cost": f"CAST(REPLACE({tblref}.cost_per_gallon_litre,'$','') AS DECIMAL(10,4))",
        "msdyn_costunit": "'usd'",
        "msdyn_evidence": "'invoice'",
        "msdyn_fuelquantity": f"CAST(REPLACE({tblref}.fuel_bill_amount,'$','') AS DECIMAL(10,4))",
        "msdyn_fuelquantityunit": "'usd'",
        "msdyn_transactiondate": f"to_date({tblref}.fuel_tkt_date,'M/d/yyyy')",
        "msdyn_vehicletype": "'medium- and heavy-duty vehicles-diesel 2007-2018'",
        "load_timestamp": f"{tblref}.load_timestamp",
        "quarantined_flag_warn": "'valid_records'",
        "quarantined_flag_drop": "'valid_records'",
        "fuel_type": f"{tblref}.fuel_source_type" 
    }
    sql_template = ''.join([get_sql_silver_base_template(), get_sql_silver_base_template_addition1(catalog, schema).strip()])
    return {**col_mapping, **get_col_mapping_shared()}, sql_template


# column mappings for ontario (silver/staging)
def get_col_mapping_mc_ontario_bronze(tablename, tblref, catalog = None, schema = None, aggtype = None):
    col_mapping = {
        "tablename": tablename,
        "tblref": tblref,
        "msdyn_name": "'ontario - clear diesel'",
        "msdyn_origincorrelationid": f"concat('ontario - clear diesel','.',md5(concat_WS('.', {tblref}.unit_cost_for_the_fuel, {tblref}.total_qty, {tblref}.vehicle_type)))",
        "msdyn_consumptionenddate": f"to_date(concat({tblref}.end_date_of_the_period,'-',year(current_date())),'d-MMM-yyyy')",
        "msdyn_consumptionstartdate": f"to_date(concat({tblref}.start_date_of_the_period,'-',year(current_date())),'d-MMM-yyyy')",
        "msdyn_dataqualitytype": "'actual'",
        "msdyn_organizationalunitid": "'international - canada'",
        "msdyn_fueltypeid": "'diesel fuel'",
        "msdyn_quantity": f"CAST(COALESCE({tblref}.total_qty , 0) AS DECIMAL(10, 4))",
        "msdyn_quantityunit": "'liters'",
        "transaction_id": "''",
        "msdyn_vin": "''",
        "msdyn_cost": f"CAST({tblref}.unit_cost_for_the_fuel AS DECIMAL(10,4))",
        "msdyn_costunit": "'cad'",
        "msdyn_evidence": "'invoice'",
        "msdyn_fuelquantity": f"CAST({tblref}.total_qty AS DECIMAL(10,4))",
        "msdyn_fuelquantityunit": "'cad'",
        "msdyn_transactiondate": f"to_date({tblref}.transaction_date,'M/d/yyyy')",
        "msdyn_vehicletype": f"{tblref}.vehicle_type",
        "load_timestamp": f"{tblref}.load_timestamp",
        "quarantined_flag_warn": "'valid_records'",
        "quarantined_flag_drop": "'valid_records'"
    }
    sql_template = get_sql_silver_base_template()
    return {**col_mapping, **get_col_mapping_shared()}, sql_template


# column mappings for quebec (silver/staging)
def get_col_mapping_mc_quebec_bronze(tablename, tblref, catalog = None, schema = None, aggtype = None):
    col_mapping = {
        "tablename": tablename,
        "tblref": tblref,
        "msdyn_name": "'quebec - clear diesel'",
        "msdyn_origincorrelationid": f"concat('quebec - clear diesel','.',md5(concat_WS('.',{tblref}.end_date_of_the_period,{tblref}.start_date_of_the_period,{tblref}.total_qty,{tblref}.unit_cost_for_the_fuel,{tblref}.total_qty,{tblref}.transaction_date)))",
        "msdyn_consumptionenddate": f"to_date(concat({tblref}.end_date_of_the_period,'-',year(current_date())),'d-MMM-yyyy')",
        "msdyn_consumptionstartdate": f"to_date(concat({tblref}.start_date_of_the_period,'-',year(current_date())),'d-MMM-yyyy')",
        "msdyn_dataqualitytype": "'actual'",
        "msdyn_organizationalunitid": "'international - canada'",
        "msdyn_fueltypeid": "'diesel fuel'",
        "msdyn_quantity": f"CAST(COALESCE({tblref}.total_qty , 0) AS DECIMAL(10, 4))",
        "msdyn_quantityunit": "'liters'",
        "transaction_id": "''",
        "msdyn_vin": "''",
        "msdyn_cost": f"CAST({tblref}.unit_cost_for_the_fuel AS DECIMAL(10,4))",
        "msdyn_costunit": "'cad'",
        "msdyn_evidence": "'invoice'",
        "msdyn_fuelquantity": f"CAST({tblref}.total_qty AS DECIMAL(10,4))",
        "msdyn_fuelquantityunit": "'cad'",
        "msdyn_transactiondate": f"to_date({tblref}.transaction_date,'M/d/yyyy')",
        "msdyn_vehicletype": f"'medium- and heavy-duty vehicles-diesel 2007-2018'",
        "load_timestamp": f"{tblref}.load_timestamp",
        "quarantined_flag_warn": "'valid_records'",
        "quarantined_flag_drop": "'valid_records'"
    }
    sql_template = get_sql_silver_base_template()
    return {**col_mapping, **get_col_mapping_shared()}, sql_template


# column mappings for jetfuel (silver/staging)
def get_col_mapping_mc_jetfuel_bronze(tablename, tblref, catalog = None, schema = None, aggtype = None):
    col_mapping = {
        "tablename": tablename,
        "tblref": tblref,
        "msdyn_name": "'jet fuel - mckfo'",
        "msdyn_origincorrelationid": f"concat('jet fuel - mckfo','.',md5(concat_WS('.',{tblref}.trip,{tblref}.leg,{tblref}.tail)))",
        "msdyn_consumptionenddate": f"to_date({tblref}.dept_dt,'M/d/yyyy')",
        "msdyn_consumptionstartdate": f"to_date({tblref}.dept_dt,'M/d/yyyy')",
        "msdyn_dataqualitytype": "'actual'",
        "msdyn_organizationalunitid": "'corporate'",
        "msdyn_fueltypeid": "'jet gasoline'",
        "msdyn_quantity": f"coalesce(CAST(replace({tblref}.fuel_burn_gall,',','') AS DECIMAL(10,4)),0 )",
        "msdyn_quantityunit": "'gallons'",
        "transaction_id": f"CONCAT({tblref}.trip,'.',{tblref}.leg,'.',{tblref}.tail)",
        "msdyn_vin": "''",
        "msdyn_cost": f"CAST(replace({tblref}.fuel_burn_lbs_gal,',','') AS DECIMAL(10,4))",
        "msdyn_costunit": "'usd'",
        "msdyn_evidence": "'invoice'",
        "msdyn_fuelquantity": f"CAST(replace({tblref}.fuel_burn_gall,',','') AS DECIMAL(10,4))",
        "msdyn_fuelquantityunit": "'usd'",
        "msdyn_transactiondate": f"to_date({tblref}.dept_dt,'M/d/yyyy')",
        "msdyn_vehicletype": f"'aircraft-jet fuel'",
        "load_timestamp": f"{tblref}.load_timestamp",
        "quarantined_flag_warn": "'valid_records'",
        "quarantined_flag_drop": "'valid_records'",
        "msdyn_distance": f"CAST(replace({tblref}.distance,',','') AS DECIMAL(10,4))"
    }
    sql_template = get_sql_silver_base_template()
    col_mapping_shared = get_col_mapping_shared()
    del col_mapping_shared["msdyn_distance"]
    return {**col_mapping, **col_mapping_shared}, sql_template


# column mappings for total fuel spend (gold)
def get_col_mapping_mobile_combustion_silver(tablename, tblref, catalog = None, schema = None, aggtype = None):
    col_mapping = {
        "tablename": tablename,
        "tblref": tblref,
        "name": "msdyn_name",
        "vehicletype": "msdyn_vehicletype",
        "month": "MONTH(msdyn_consumptionstartdate)",
        "year": "YEAR(msdyn_consumptionstartdate)",
        "quantity": "msdyn_quantity",
        "quantityunit": "msdyn_quantityunit",
        "cost": "msdyn_cost",
        "costunit": "msdyn_costunit",
        "totalfuelcost": "ROUND(msdyn_quantity * msdyn_cost, 2)",
        "totalfuelcostunit": "msdyn_costunit"
    }
    if aggtype == "monthly":
        sql_template = get_sql_gold_base_fuel_spend_monthly_template()
    else: sql_template = get_sql_gold_base_fuel_spend_yearly_template()
    return {**col_mapping}, sql_template