# parameterized transformation silver base template
def get_sql_silver_base_template():
    SQL = """
        SELECT
            <msdyn_name> AS msdyn_name,
            <msdyn_origincorrelationid> as msdyn_origincorrelationid, 
            <msdyn_consumptionenddate> AS msdyn_consumptionenddate,
            <msdyn_consumptionstartdate> AS msdyn_consumptionstartdate,
            <msdyn_dataqualitytype> AS msdyn_dataqualitytype,
            <msdyn_organizationalunitid> AS msdyn_organizationalunitid,
            <msdyn_fueltypeid> AS msdyn_fueltypeid,
            <msdyn_quantity> AS msdyn_quantity,
            <msdyn_quantityunit> AS msdyn_quantityunit,
            <transaction_id> AS transaction_id,
            <msdyn_vin> AS msdyn_vin,
            <msdyn_cost> as msdyn_cost,
            <msdyn_costunit> AS msdyn_costunit,
            <msdyn_evidence> AS msdyn_evidence,
            <msdyn_fuelquantity> AS msdyn_fuelquantity,
            <msdyn_fuelquantityunit> AS msdyn_fuelquantityunit,
            <msdyn_transactiondate> AS msdyn_transactiondate,
            <msdyn_vehicletype> AS msdyn_vehicletype,
            <msdyn_fueladdress> as msdyn_fueladdress,
            <msdyn_fuelzip> as msdyn_fuelzip,	
            <msdyn_distance> as msdyn_distance,
            <msdyn_facilityId> as msdyn_facilityId,
            <msdyn_valuechainpartnerid> as msdyn_valuechainpartnerid,
            <load_timestamp> as load_timestamp,
            <quarantined_flag_warn> as quarantined_flag_warn,
            <quarantined_flag_drop> as quarantined_flag_drop
        FROM <tablename> AS <tblref>
    """
    return SQL

# parameterized transformation silver base template addition 1
def get_sql_silver_base_template_addition1(catalog, schema):
    SQL = f"""
        LEFT JOIN 
        (SELECT * FROM 
        (SELECT source_fuel_type_name, msm_fuel_type_name, 
        row_number() over (PARTITION BY source_fuel_type_name, msm_fuel_type_name ORDER BY source_fuel_type_name) as Rn
        FROM {catalog}.{schema}.fuel_type_msm_lookup)
        WHERE Rn = 1) as f
        ON UPPER(TRIM(<fuel_type>)) = UPPER(TRIM(f.source_fuel_type_name))
        WHERE (f.msm_fuel_type_name not like '%No GHG Calculation%' or f.msm_fuel_type_name IS NULL)
    """
    return SQL

# parameterized transformation gold base template (monthly fuel spend)
def get_sql_gold_base_fuel_spend_monthly_template():
    SQL = f"""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS pk_id, 
            <tblref>.year as pk_year,
            <tblref>.month as pk_month, 
            <tblref>.name as pk_name,
            <tblref>.vehicletype as pk_vehicletype,
            SUM(<tblref>.quantity) AS fuel_quantity,
            <tblref>.quantityunit AS fuel_quantity_unit,
            SUM(<tblref>.totalfuelcost) AS total_fuel_cost,
            <tblref>.totalfuelcostunit as total_fuel_cost_unit
        FROM (
            SELECT
                <name> AS name,
                <vehicletype> AS vehicletype, 
                <month> AS month,
                <year> AS year,
                <quantity> AS quantity,
                <quantityunit> AS quantityunit,
                <cost> as cost,
                <costunit> as costunit,
                <totalfuelcost> AS totalfuelcost,
                <totalfuelcostunit> as totalfuelcostunit
            FROM <tablename>
            WHERE <quantity> > 0 AND <cost> > 0
        ) <tblref>
        GROUP BY <tblref>.year, <tblref>.month, <tblref>.name, <tblref>.vehicletype, <tblref>.quantityunit, <tblref>.totalfuelcostunit
        ORDER BY <tblref>.name ASC, <tblref>.year DESC, <tblref>.month ASC
    """
    return SQL

# parameterized transformation gold base template (yearly fuel spend)
def get_sql_gold_base_fuel_spend_yearly_template():
    SQL = f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS pk_id, 
            <tblref>.year as pk_year,
            <tblref>.name as pk_name,
            <tblref>.vehicletype as pk_vehicletype,
            SUM(<tblref>.quantity) AS fuel_quantity,
            <tblref>.quantityunit AS fuel_quantity_unit,
            SUM(<tblref>.totalfuelcost) AS total_fuel_cost,
            <tblref>.totalfuelcostunit as total_fuel_cost_unit
        FROM (
            SELECT
                <name> AS name,
                <vehicletype> AS vehicletype, 
                <year> AS year,
                <quantity> AS quantity,
                <quantityunit> AS quantityunit,
                <cost> as cost,
                <costunit> as costunit,
                <totalfuelcost> AS totalfuelcost,
                <totalfuelcostunit> as totalfuelcostunit
            FROM <tablename>
            WHERE <quantity> > 0 AND <cost> > 0
        ) <tblref>
        GROUP BY <tblref>.year, <tblref>.name, <tblref>.vehicletype, <tblref>.quantityunit, <tblref>.totalfuelcostunit
        ORDER BY <tblref>.name ASC, <tblref>.year DESC
    """
    return SQL