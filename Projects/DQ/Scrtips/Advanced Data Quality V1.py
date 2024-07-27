# Databricks notebook source
# DBTITLE 1,Library Setup
# Import Libraries
import os
import json
from datetime import datetime
import pandas as pd
from functools import reduce
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import dq_lib as dq
dbutils.widgets.text(name = "src_name", defaultValue = 'all', label = "Source System")

# COMMAND ----------

# DBTITLE 1,Environment Setup
# Setup the config files path and metrics database
dev = True

if dev is True:
    config_path = '/dbfs/FileStore/TheSparklers/config_v1'
    config_file = 'dq_config.json'
else:
    config_path = ''
    config_file = ''
src_sys = dbutils.widgets.get("src_name")

# COMMAND ----------

# DBTITLE 1,Load JSON config files
def parse_rules_config(src_sys, config_path, file_name):
    """
    Parse the rules JSON file and coverts to a dataframe
    
    Args
    ----
    src_sys : Name of the source system
    config_path: Path to configuration directory
    file_name: Name of the input file

    Returns
    -------
    df: dataframe with all the active rules
    """
    with open(os.path.join(config_path,file_name)) as f:
        dq_rule_json = json.load(f)
        dq_rule_raw = pd.json_normalize(dq_rule_json,'rules',['source_system', 'database_name','table_name','primary_id'])
        dq_rule = dq_rule_raw[dq_rule_raw['rule_status'] == 'active']
        dq_rule['column_name'] = dq_rule['column_name'].str.split(',')
        dq_rule = dq_rule.explode('column_name')
        if src_sys.lower() != 'all':
            dq_rule['source_system'] = dq_rule['source_system'].str.lower()
            dq_rule = dq_rule[dq_rule['source_system'] == src_sys]
        if len(dq_rule) == 0: raise Exception('No records found in rules dataframe')
        return dq_rule

# COMMAND ----------

def parse_templates_config(config_path, file_name):
    """
    Parse the query & API templates JSON file and covert to a dataframe
    
    Args
    ----
    config_path: Path to configuration directory
    file_name: Name of the input file

    Returns
    -------
    df: dataframe with all the active query templates
    """
    with open(os.path.join(config_path,'dq_templates.json')) as f:
        dq_templates_json = json.load(f)
        dq_templates = pd.json_normalize(dq_templates_json['sql_template'],'templates',['merge_ind'])
        dq_queries = pd.json_normalize(dq_templates_json['sql'])
        dq_apis = pd.json_normalize(dq_templates_json['api'])
    return dq_templates, dq_queries, dq_apis

# COMMAND ----------

# DBTITLE 1,Metadata Validation
def metadata_validation(spark, pd, dq_rule):
    """
    Function to validate all the input databases, tables and columns that are in data quality
    monitoring scope are present in the metadata catalog.
    
    Args
    ----
    spark: Name of the spark session
    pd: Alias name of the pandas library
    dq_rue: Name of the input rules dataframe
    
    Returns:
    df: Dataframe with all the rules in scope
    """
    # Ensure all the databases, tables and columns specified in the rules file is present in catalog
    scope_dict = []
    sources = dq_rule['source_system'].unique().tolist()
    for src in sources:
        dbs = dq_rule[dq_rule['source_system'] == src]['database_name'].unique().tolist()
        for db in dbs:
            # validate if the database in dq scope is present in catalog
            if spark.catalog.databaseExists(db):
                db_tbls=[]
                for db_tbl in spark.catalog.listTables(db):
                    db_tbls += [db_tbl[0]]
                db_tbls = list(map(str.lower,db_tbls))
                tbls = dq_rule[(dq_rule['source_system'] == src) & 
                               (dq_rule['database_name'] == db)]['table_name'].unique().tolist()
                for tbl in tbls:
                    # validate if table in dq scope is present in catalog
                    if tbl in db_tbls:
                        tbl_metadata = spark.catalog.listColumns(f'{db}.{tbl}')
                        db_cols = {}
                        for db_col in tbl_metadata:
                            db_cols[db_col[0].lower()] =  db_col[2]
                        cols = dq_rule[(dq_rule['source_system'] == src) &
                                       (dq_rule['database_name'] == db) &
                                       (dq_rule['table_name'] == tbl) &
                                       (dq_rule['rule_scope'] == 'column')]['column_name'].unique().tolist()
                        if len(cols) >=1:                       
                            for col in cols:
                                # validate if the column in dq scope is present in catalog
                                if col in list(db_cols.keys()):
                                    scope_dict += [{'src_sys':src, 'db_name':db, 'tbl_name': tbl,
                                                    'col_name':col, 'data_type':db_cols[col]}]
                                else:
                                    raise ValueError(f'{db}.{tbl}.{col} column not present for {src}')
                        # collect the columns where dq rules defined at table level
                        tab_lvl_dq = dq_rule[(dq_rule['source_system'] == src) &
                                             (dq_rule['database_name'] == db) &
                                             (dq_rule['table_name'] == tbl) &
                                             (dq_rule['rule_scope'] == 'table')]['table_name'].unique().tolist()
                        if len(tab_lvl_dq) >=1:
                            for col in tbl_metadata:
                                scope_dict += [{'src_sys':src, 'db_name':db, 'tbl_name': tbl, 'col_name':col[0].lower(),
                                               'data_type':col[2]}]
                    else:
                        raise ValueError(f'{db}.{tbl} table not present for {src}')
            else:
                raise ValueError(f'{db} database not present for {src}')
    # convert the scope dictionary into a dataframe
    dq_scope = pd.DataFrame.from_dict(scope_dict).drop_duplicates().reset_index(drop=True)
    print('Metadata validation check completed')
    return dq_scope

# COMMAND ----------

# DBTITLE 1,Rule validation
def rule_validation(pd, dq_rule, dq_templates, dq_queries, dq_apis, dq_scope):
    """
    Function to validate the the input rules
    
    Args
    ----
    pd: Alias name of the pandas instance
    dq_rule: Name of the rules dataframe
    dq_templates: Name of the query templates dataframe
    dq_queries: Name of the queries dataframe
    dq_apis: Name of the apis dataframe
    dq_scope: Name of the quality check scope dataframe
    """
    sources = dq_rule['source_system'].unique().tolist()
    rules_subset = ['rule_name', 'rule_scope', 'column_name','query_template', 'params','query','api_template']
    for src in sources:
        dbs = dq_rule[dq_rule['source_system'] == src]['database_name'].unique().tolist()
        for db in dbs:
            tbls = dq_rule[(dq_rule['source_system'] == src) &
                           (dq_rule['database_name'] == db)]['table_name'].unique().tolist()
            for tbl in tbls:
                rules = dq_rule[(dq_rule['source_system'] == src) &
                                (dq_rule['database_name'] == db) & 
                                (dq_rule['table_name'] == tbl)][rules_subset]
                meta_data = dq_scope[(dq_scope['src_sys'] == src) &
                                        (dq_scope['db_name'] == db) & 
                                        (dq_scope['tbl_name'] == tbl)][['col_name', 'data_type']]
                for rule in rules.itertuples():
                    if ((rule.query_template == dq_templates['query_template']).any() 
                            or (rule.api_template == dq_apis['api_template']).any()):
                        if pd.isnull(rule.api_template):
                            scope = dq_templates[dq_templates['query_template'] == rule.query_template]['query_scope'].values[0]
                        else:
                            scope = dq_apis[dq_apis['api_template'] == rule.api_template]['api_scope'].values[0]
                        # collect the columns in scope
                        col_list = []
                        if rule.rule_scope == 'table':
                            col_list += [meta_data['col_name'].unique().tolist()]
                        else:
                            col_list += [rule.column_name]
                        if scope != 'any':
                            for col in col_list:
                                col_metadata = meta_data[meta_data['col_name'] == col]['data_type'].values[0]
                                if col_metadata in scope.split(','):
                                    pass # valid query or api template
                                else:
                                    raise Exception(f'{rule.rule_name} is not applicable for {src}.{db}.{tbl}.{col}')
                    elif(rule.query == dq_queries['query']).any():
                        pass # valid query
                    else:
                        raise Exception(f'Query or API template is not present for {rule.rule_name} in {src}.{db}.{tbl}')
    print('Rule validation completed')
    return

# COMMAND ----------

# DBTITLE 1,Metadata Lookup
def metadata_lookup(spark, pd, dq_scope, dq_rule):
    """
    Function to lookup metadata all the input databases, tables and columns that are in data quality
    monitoring scope and create respective views for metadata loads.
    
    Args
    ----
    spark: Name of the spark session
    pd: Alias name of the pandas library
    dq_scope: Name of the input rules dataframe
    dq_rue: Name of the input rules dataframe
    """  
    db_dict = []
    tbl_dict = []
    col_dict = []
    sources = dq_scope['src_sys'].unique().tolist()
    for src in sources:
        dbs = dq_scope[dq_scope['src_sys'] == src]['db_name'].unique().tolist()
        for db in dbs:
            for cdb in spark.catalog.listDatabases():
                if db == cdb[0].lower():
                    # collect database metadata
                    db_dict += [{'src_sys':src, 'db_name':db, 'db_catalog':cdb[1], 'db_desc':cdb[2], 'db_loc':cdb[3]}] 
                    tbls = dq_scope[(dq_scope['src_sys'] == src) & 
                               (dq_scope['db_name'] == db)]['tbl_name'].unique().tolist()
                    for tbl in tbls:
                        for ctbl in spark.catalog.listTables(db):
                            if tbl == ctbl[0].lower():
                                tbl_cnt = spark.sql(f'''select count(1) as cnt from {db}.{tbl}''').collect()[0][0]
                                df_col_meta = spark.catalog.listColumns(f'{db}.{tbl}')
                                col_cnt = len(df_col_meta)
                                # collect table metadata
                                tbl_dict += [{'src_sys':src, 'db_name':db,'tbl_name':tbl,
                                              'tbl_desc':ctbl[3],'tbl_type':ctbl[4], 'tbl_istemp': ctbl[5], 
                                              'tbl_rec_cnt':tbl_cnt, 'col_cnt':col_cnt}]
                                cols = dq_scope[(dq_scope['src_sys'] == src) & 
                                                (dq_scope['db_name'] == db) &
                                                (dq_scope['tbl_name'] == tbl)]['col_name'].unique().tolist()
                                for col in cols:
                                    for ccol in df_col_meta:
                                        if col == ccol[0].lower():
                                            # collect column metadata
                                            col_dict += [{'src_sys':src, 'db_name':db,'tbl_name':tbl, 'col_name':col,
                                                         'col_desc': ccol[1], 'data_type':ccol[2], 'is_nullable':ccol[3],
                                                          'is_partition': ccol[4], 'is_bucket':ccol[5]}]
    # create views for db metadata
    df_db_dict = pd.DataFrame.from_dict(db_dict).drop_duplicates().reset_index(drop=True)
    spark.createDataFrame(df_db_dict).createOrReplaceTempView('vw_db_dict')
    # create views for table metadata
    df_tbl_dict = pd.DataFrame.from_dict(tbl_dict).drop_duplicates().reset_index(drop=True)
    spark.createDataFrame(df_tbl_dict).createOrReplaceTempView('vw_tbl_dict')
    # create views for column metadata
    df_col_dict = pd.DataFrame.from_dict(col_dict).drop_duplicates().reset_index(drop=True)
    spark.createDataFrame(df_col_dict).createOrReplaceTempView('vw_col_dict')
    # create staging table and views for rules metadata
    df_dq_rule = dq_rule[['rule_name','rule_dim']].drop_duplicates().reset_index(drop=True)
    spark.createDataFrame(df_dq_rule).createOrReplaceTempView('vw_dq_rule')
    print('Metadata views are created')
    return

# COMMAND ----------

# DBTITLE 1,Load Metadata Tables
def load_db_metadata(spark, metrics_db, meta_db):
    """
    Function to load the database metadata table in upsert mode through change capture process
    
    Args
    ----
    spark: Name of the spark session
    metrics_db: Name of the metrics database
    meta_db: Name of the database metadata table
    """
    
    # create view to lookup the db_id
    spark.sql(f'''
    CREATE OR REPLACE TEMP VIEW vw_db_src_stg AS 
    SELECT 
    b.db_id,
    a.db_name,
    a.src_sys,
    a.db_catalog,
    a.db_desc,
    a.db_loc
    FROM vw_db_dict a
    LEFT JOIN {metrics_db}.{meta_db} b 
    ON a.db_name = b.db_name 
    AND a.src_sys = b.src_name''')
    
    # Load the data to target table
    df = spark.sql(f'''
    MERGE INTO {metrics_db}.{meta_db} tgt
    USING vw_db_src_stg stg
    ON tgt.db_id = stg.db_id
    WHEN MATCHED AND
      (tgt.db_catalog <> stg.db_catalog OR
       tgt.db_desc <> stg.db_desc OR 
       tgt.db_loc <> stg.db_loc) THEN 
       UPDATE SET
         tgt.db_catalog = stg.db_catalog,
         tgt.db_desc = stg.db_desc,
         tgt.db_loc = stg.db_loc,
         tgt.last_updated_on = current_date()
    WHEN NOT MATCHED THEN
      INSERT (
      db_name,
      src_name,
      db_catalog,
      db_desc,
      db_loc,
      created_on,
      last_updated_on
      ) VALUES(
      stg.db_name,
      stg.src_sys,
      stg.db_catalog,
      stg.db_desc,
      stg.db_loc,
      current_date(),
      current_date())''')
    
    # Summary
    status = df.collect()[0]
    print(f'''Table: {meta_db}, Insert: {status['num_inserted_rows']}, Update:{status['num_updated_rows']}''')
    return

# COMMAND ----------

def load_tbl_metadata(spark, metrics_db, meta_db, meta_tbl):
    """
    Function to load the table metadata in upsert mode through change capture process
    
    Args
    ----
    spark: Name of the spark session
    metrics_db: Name of the metrics database
    meta_db: Name of the database metadata table
    meta_tbl: Name of the table metadata table
    """
    
    # Create view to lookup the db and table ids
    spark.sql(f'''
    CREATE OR REPLACE TEMP VIEW vw_tbl_stg AS
    SELECT 
    b.db_id,
    c.tbl_id,
    a.tbl_name,
    a.tbl_desc,
    lower(a.tbl_type) as tbl_type,
    a.tbl_istemp,
    a.tbl_rec_cnt,
    a.col_cnt
    FROM vw_tbl_dict a
    INNER JOIN {metrics_db}.{meta_db} b
    ON a.src_sys = b.src_name 
    AND a.db_name = b.db_name
    LEFT JOIN {metrics_db}.{meta_tbl} c
    ON b.db_id = c.db_id 
    AND a.tbl_name = c.tbl_name
    ''')
    
    # Load the data to target table
    df = spark.sql(f'''
    MERGE INTO {metrics_db}.{meta_tbl} tgt
    USING vw_tbl_stg stg
    ON tgt.tbl_id = stg.tbl_id
    WHEN MATCHED AND 
      (
      tgt.tbl_desc <> stg.tbl_desc OR
      tgt.tbl_type <> stg.tbl_type OR
      tgt.is_temp <> stg.tbl_istemp OR
      tgt.curr_rec_cnt <> stg.tbl_rec_cnt OR
      tgt.curr_col_cnt <> stg.col_cnt) THEN
      UPDATE SET
        tgt.tbl_desc = stg.tbl_desc,
        tgt.tbl_type = stg.tbl_type,
        tgt.is_temp = stg.tbl_istemp,
        tgt.curr_rec_cnt = stg.tbl_rec_cnt,
        tgt.curr_col_cnt = stg.col_cnt,
        tgt.last_updated_on = current_date()
    WHEN NOT MATCHED THEN
      INSERT (
      db_id,
      tbl_name,
      tbl_desc,
      tbl_type,
      is_temp,
      curr_rec_cnt,
      curr_col_cnt,
      created_on,
      last_updated_on
      )VALUES(
      stg.db_id,
      stg.tbl_name,
      stg.tbl_desc,
      stg.tbl_type,
      stg.tbl_istemp,
      stg.tbl_rec_cnt,
      stg.col_cnt,
      current_date(),
      current_date())''')
    
    # Summary
    status = df.collect()[0]
    print(f'''Table: {meta_tbl}, Insert: {status['num_inserted_rows']}, Update:{status['num_updated_rows']}''')
    return

# COMMAND ----------

def load_col_metadata(spark, metrics_db, meta_db, meta_tbl, meta_col):
    """
    Function to load the column metadata table in upsert mode through change capture process
    
    Args
    ----
    spark: Name of the spark session
    metrics_db: Name of the metrics database
    meta_db: Name of the database metadata table
    meta_tbl: Name of the table metadata table
    meta_col: Name of the column metadata table
    """
    # Create view to lookup the db, table and column ids
    spark.sql(f'''
    CREATE OR REPLACE TEMP VIEW vw_col_stg AS
    SELECT
    d.col_id,
    c.tbl_id,
    a.col_name,
    a.col_desc,
    a.data_type,
    a.is_nullable,
    a.is_partition,
    a.is_bucket
    FROM vw_col_dict a
    INNER JOIN {metrics_db}.{meta_db} b
    ON a.src_sys = b.src_name 
    AND a.db_name = b.db_name
    INNER JOIN {metrics_db}.{meta_tbl} c
    ON b.db_id = c.db_id 
    AND a.tbl_name = c.tbl_name
    LEFT JOIN {metrics_db}.{meta_col} d
    ON c.tbl_id = d.tbl_id
    AND a.col_name = d.col_name''')
    
    # Load the data to target table
    df = spark.sql(f'''
    MERGE INTO {metrics_db}.{meta_col} tgt
    USING vw_col_stg stg 
    ON tgt.col_id = stg.col_id
    WHEN MATCHED AND 
      (
      tgt.col_name <> stg.col_name OR
      tgt.col_desc <> stg.col_desc OR
      tgt.data_type <> stg.data_type OR
      tgt.is_nullable <> stg.is_nullable OR
      tgt.is_partition <> stg.is_partition OR
      tgt.is_bucket <> stg.is_bucket) THEN
      UPDATE SET
      tgt.col_desc = stg.col_desc,
      tgt.data_type = stg.data_type,
      tgt.is_nullable = stg.is_nullable,
      tgt.is_partition = stg.is_partition,
      tgt.is_bucket = stg.is_bucket,
      tgt.last_updated_on = current_date()
    WHEN NOT MATCHED THEN
      INSERT (
      tbl_id,
      col_name,
      col_desc,
      data_type,
      is_nullable,
      is_partition,
      is_bucket,
      created_on,
      last_updated_on
      )
      VALUES (
      stg.tbl_id,
      stg.col_name,
      stg.col_desc,
      stg.data_type,
      stg.is_nullable,
      stg.is_partition,
      stg.is_bucket,
      current_date(),
      current_date())''')
    # Summary
    status = df.collect()[0]
    print(f'''Table: {meta_col}, Insert: {status['num_inserted_rows']}, Update:{status['num_updated_rows']}''')
    return    

# COMMAND ----------

def load_rule_metadata(spark, metrics_db, meta_rule):
    """
    Function to load the rule metadata table in upsert mode through change capture process
    
    Args
    ----
    spark: Name of the spark session
    metrics_db: Name of the metrics database
    dim_rule: Name of the rule metadata table
    """
    
    # create to lookup the rule id
    spark.sql(f'''
    CREATE OR REPLACE TEMP VIEW stg_dq_rules AS
    SELECT
    b.rule_id,
    a.rule_name,
    a.rule_dim
    FROM (SELECT 
          DISTINCT rule_name, rule_dim 
          FROM vw_dq_rule) a
    LEFT JOIN {metrics_db}.{meta_rule} b 
    ON a.rule_name = b.rule_name''')

    # Load the data to target table
    df = spark.sql(f'''
    MERGE INTO {metrics_db}.{meta_rule} tgt
    USING stg_dq_rules stg
    ON tgt.rule_id = stg.rule_id
    WHEN MATCHED AND tgt.dq_dim_name <> stg.rule_dim THEN
      UPDATE SET
        tgt.dq_dim_name = stg.rule_dim,
        tgt.last_updated_on = current_date()
    WHEN NOT MATCHED THEN
      INSERT (rule_name, 
    dq_dim_name,
    created_on,
    last_updated_on)
      VALUES(
      stg.rule_name, 
      stg.rule_dim, 
      current_date(),
      current_date())''')
    # Summary
    status = df.collect()[0]
    print(f'''Table: {meta_rule}, Insert: {status['num_inserted_rows']}, Update:{status['num_updated_rows']}''')
    return 

# COMMAND ----------

# DBTITLE 1,Lookup Column and Rule Ids
def lookup_column_ids(spark, metrics_db, meta_db, meta_tbl, meta_col):
    '''
    Function to lookup the column ids for all the columns in dq scope
    
    Args
    ----
    spark: Name of the spark session
    metrics_db: Name of the metrics database
    meta_db: Name of the database metadata table
    meta_tbl: Name of the table metadata table
    meta_col: Name of the column metadata table
    
    Returns
    --------
    df: Returns the the column level metadata dataframe
    '''
    
    meta_data_query = f'''
    SELECT
    c.src_name,
    c.db_name,
    b.tbl_name,
    b.curr_rec_cnt,
    a.col_name,
    a.col_id,
    a.data_type
    FROM 
    {metrics_db}.{meta_col} a 
    INNER JOIN {metrics_db}.{meta_tbl} b ON a.tbl_id = b.tbl_id
    INNER JOIN {metrics_db}.{meta_db} c ON b.db_id = c.db_id'''
    df_metadata = spark.sql(meta_data_query).toPandas()
    return df_metadata

# COMMAND ----------

def lookup_rule_ids(spark, metrics_db, meta_rule):
    '''
    Function to lookup the rules ids for all the dq rules in scope scope
    
    Args
    ----
    spark: Name of the spark session
    metrics_db: Name of the metrics database
    dim_rule: Name of the rule metadata table
    
    Returns
    -------
    df: Returns the rule metadata dataframe
    '''
    
    rule_metadata_query = f'''
    select 
    rule_id,
    rule_name,
    dq_dim_name 
    from {metrics_db}.{meta_rule} '''
    df_rule_metadata = spark.sql(rule_metadata_query).toPandas()
    return df_rule_metadata

# COMMAND ----------

# DBTITLE 1,Rules Parser
def rules_parser(spark, pd, src_sys, config_path, config_file):
    """
    Wrapper function which loads the DQ rule config files, validate the input database, 
    table and columns against spark metadata catalog and loads the metadata to metadata tables.
    
    Args
    ----
    spark: Name of the spark session
    pd: Alias name of the pandas library
    src_sys : Name of the source system
    config_path: Path to configuration directory
    config_file: Name of the dq process config gile
    
    Returns
    -------
    df: Dataframes - rule, query template, query, column level and rule level metadata
    config: dq config json object
    """
    with open(os.path.join(config_path,config_file)) as f:
        config = json.load(f)
    
    # Load the variables from config file
    rules_file = config['dq_config']['rules_file']
    template_file = config['dq_config']['query_template_file']
    metrics_db = config['dq_metrics']['dq_metrics_db']
    meta_db = config['dq_metrics']['dq_meta_db']
    meta_tbl = config['dq_metrics']['dq_meta_tbl']
    meta_col = config['dq_metrics']['dq_meta_col']
    meta_rule = config['dq_metrics']['dq_meta_rule']
    metrics = config['dq_metrics']['dq_metrics']
    
    # Load rules config files
    dq_rule = parse_rules_config(src_sys, config_path, rules_file)
    dq_templates, dq_queries, dq_apis = parse_templates_config(config_path, template_file)
    print('Loaded DQ Rule config files')
    
    # validate input database, table and columns against metadata catalog
    dq_scope = metadata_validation(spark, pd, dq_rule)
    
    # validate the input rules
    rule_validation(pd, dq_rule, dq_templates, dq_queries, dq_apis, dq_scope)
    
    # create metadata views
    metadata_lookup(spark, pd, dq_scope, dq_rule)
    
    # Load metadata tables
    print('Load Metadata Tables:')
    load_db_metadata(spark, metrics_db, meta_db)
    load_tbl_metadata(spark, metrics_db, meta_db, meta_tbl)
    load_col_metadata(spark, metrics_db, meta_db, meta_tbl, meta_col)
    load_rule_metadata(spark, metrics_db, meta_rule)
    
    # lookup col_id and rule_id
    df_metadata = lookup_column_ids(spark, metrics_db, meta_db, meta_tbl, meta_col)
    df_rule_metadata = lookup_rule_ids(spark, metrics_db, meta_rule)
    
    return dq_rule, dq_templates, dq_queries, dq_apis, df_metadata, df_rule_metadata, config

# COMMAND ----------

# DBTITLE 1,Query Generator
def query_genarator(dq_rule, dq_templates, dq_queries, df_metadata, df_rule_metadata):
    """
    Function to generate the data quality check queries based on the given rules
    
    Args
    ----
    dq_rule: Name of the rules dataframe
    dq_templates: Name of the query template dataframe
    dq_queries: Name of the queries dataframe
    df_metadata: Name of the column level metadata dataframe
    df_rule_metadata: Name of the rule level metadata dataframe
    
    Returns
    -------
    list: List of generated validation queries
    """
    
    # Helper function
    def replace_expr(template, params):
        """
        Helper function to replace the expressions
        """
        for key, value in params.items():
            from_str = '{'+key+'}' 
            template = template.replace(from_str, value)
        return template
    
    # Generate queries
    dq_metric_queries = []
    track_failed_rules = []
    rules_subset = ['rule_name', 'rule_scope', 'column_name','query_template', 'params','query','track_failed_rec', 'primary_id']
    sources = dq_rule['source_system'].unique().tolist()
    for src in sources:
        dbs = dq_rule[dq_rule['source_system'] == src]['database_name'].unique().tolist()
        for db in dbs:
            tbls = dq_rule[(dq_rule['source_system'] == src) &
                           (dq_rule['database_name'] == db)]['table_name'].unique().tolist()
            for tbl in tbls:
                query_list = []
                # get the list of rules at table level and perform iteration
                rules = dq_rule[(dq_rule['source_system'] == src) &
                                (dq_rule['database_name'] == db) & 
                                (dq_rule['table_name'] == tbl)][rules_subset]
                rules['params'] = rules['params'].fillna('')
                # get the primary id from input rule and construt select statement
                primary_key_inp = rules['primary_id'].unique().tolist()[0]
                primary_key_inp = primary_key_inp if primary_key_inp !='' else 'NULL'
                primary_key_list = primary_key_inp.split(',')
                if len(primary_key_list) > 1:
                    primary_id = f'''concat_ws('_',{','.join(primary_key_list)})'''
                else:
                    primary_id = primary_key_list[0]
                # extract column id from metadata
                meta_data = df_metadata[(df_metadata['src_name'] == src) &
                                        (df_metadata['db_name'] == db) & 
                                        (df_metadata['tbl_name'] == tbl)][['col_name', 'col_id', 'data_type']]
                for rule in rules.itertuples():
                    rule_id = df_rule_metadata[df_rule_metadata['rule_name'] == rule.rule_name]['rule_id'].values[0] 
                    # collect the columns in scope
                    col_list = []
                    if rule.rule_scope == 'table':
                        col_list += list(zip(meta_data['col_name'],meta_data['col_id']))
                    else:
                        col_list += [(rule.column_name, meta_data[meta_data['col_name'] == rule.column_name]['col_id'].values[0])]
                    # check if the rule is present in the query template
                    if (rule.query_template == dq_templates['query_template']).any():
                        query_template = dq_templates[dq_templates['query_template'] == rule.query_template]['query'].values[0]
                        merge_ind = dq_templates[dq_templates['query_template'] == rule.query_template]['merge_ind'].values[0]
                        if merge_ind == 'Y':
                            if rule.params != '':
                                param_dict = rule.params[0].copy()
                                query_template = replace_expr(query_template, param_dict)
                            for col_name, col_id in col_list:
                                select_query = query_template.replace('{col_name}', col_name)
                                query_list += [f'{select_query} as {col_id}_{rule_id}']
                                # collect the list of rules and columns to track the failed dq
                                if rule.track_failed_rec == 'Y':
                                    track_failed_rules +=[f'{col_id}_{rule_id}']
                        else:
                            if rule.params != '':
                                param_dict = rule.params[0].copy()
                            else:
                                param_dict = {}
                            param_dict.update({'db':db, 'tbl':tbl,'col_name':rule.column_name,
                                                   'col_id':str(col_list[0][1]), 'rule_id':str(rule_id),
                                                   'primary_id':primary_id})
                            query = replace_expr(query_template, param_dict)
                            if len(query) > 0:
                                dq_metric_queries += [query]
                            if rule.track_failed_rec == 'Y':
                                track_failed_rules += [f'{str(col_list[0][1])}_{str(rule_id)}']
                    #fetch query from rules file
                    elif(rule.query == dq_queries['query']).any():
                        query = dq_queries[dq_queries['query'] == rule.query]['sql'].values[0]
                        dq_metric_queries += [query.replace('{col_id}', str(col_list[0][1])).replace('{rule_id}',str(rule_id)).replace('{primary_id}',str(primary_id))]
                        if rule.track_failed_rec == 'Y':
                            track_failed_rules +=[f'{str(col_list[0][1])}_{str(rule_id)}']
                # construct the final query at table level(applicable for rules with merge_ind = 'Y')
                if len(query_list) > 0:
                    query_list += [f'{primary_id} as pkey']
                    select_query = ', '.join(query_list)
                    dq_metric_queries += [f'select {select_query} from {db}.{tbl}']
    if len(dq_metric_queries) > 0:
        print(f'{len(dq_metric_queries)} queries are generated')
    return dq_metric_queries,track_failed_rules

# COMMAND ----------

# DBTITLE 1,Query Executor
def query_executor(spark, pd, dq_metric_queries, track_failed_rules):
    """
    Function to execute auto generated data quality queries and capture the results in a pandas dataframe
    
    Args
    ----
    spark: Name of the spark session;
    pd: Alias names of the padans library
    dq_metric_quries: List of data quality queries to be executed
    track_failed_rules: List of data quality checked failed records to be tracked
    Return
    ------
    dq_results: dataframe with data quality check results
    failed_rec_dfs: Dictionary of data quality failed records
    """

    # define empty dataframe to capture the results
    dq_results = pd.DataFrame(columns = ['col_id', 'rule_id', 'dq_value'])
    failed_rec_dfs = {}

    if len(dq_metric_queries) > 0:
        # execute queries
        for query in dq_metric_queries:
            # execute query against database
            df = spark.sql(query)
            # create temporary view
            df.createOrReplaceTempView('df_vw')
            
            ###########################################
            # Capture the data quality metrics
            ###########################################
            col_list = df.columns
            col_list.remove('pkey')
            metrics_col_list = ', '.join([f'SUM({col}) AS {col}' for col in col_list])
            metric_query = f'''SELECT {metrics_col_list} FROM df_vw'''
            df_metric = spark.sql(metric_query).toPandas()
            # melt the results and clean the records
            df_metric = df_metric.melt(value_vars =df_metric.columns, var_name = 'metric',
                                        value_name = 'dq_value')
            df_metric[['col_id', 'rule_id']] = df_metric['metric'].str.split('_', expand = True)
            df_metric = df_metric.drop(columns=['metric'])
            df_metric = df_metric[['col_id','rule_id','dq_value']]
            dq_results = pd.concat([dq_results, df_metric],  ignore_index=True)

            ############################################
            # Track the DQ Failed check records
            ############################################
            dq_fail_col_list = set(col_list).intersection(set(track_failed_rules))
            if len(dq_fail_col_list) > 0:
                for col_rule in dq_fail_col_list:
                    failed_rec_dfs[col_rule] = spark.sql(f'''select '{col_rule}' as col_rule, 
                                     pkey as primary_key from df_vw where {col_rule} ==0''')
        print(f'{len(dq_metric_queries)} queries are executed')
    return dq_results,failed_rec_dfs

# COMMAND ----------

# DBTITLE 1,API Executor
def api_executor(spark, pd, dq_rule, dq_apis, df_metadata, df_rule_metadata):
    """
    Function to execute the API calls to perform API checks
    
    Args
    ----
    spark: Name of the spark session
    pd: Alias name of the padas library
    dq_rule: Name of the rules dataframe
    dq_apis: Name of the api template dataframe
    df_metadata: Name of the column level metadata dataframe
    df_rule_metadata: Name of the rule level metadata dataframe
    """
    api_results = pd.DataFrame(columns = ['col_id', 'rule_id', 'dq_value'])
    result_dict = []
    rules_subset = ['rule_name', 'rule_scope', 'column_name','api_template', 'params']
    dq_rule = dq_rule[dq_rule.api_template.notnull()]
    if len(dq_rule) > 0:
        sources = dq_rule['source_system'].unique().tolist()
        for src in sources:
            dbs = dq_rule[dq_rule['source_system'] == src]['database_name'].unique().tolist()
            for db in dbs:
                tbls = dq_rule[(dq_rule['source_system'] == src) &
                               (dq_rule['database_name'] == db)]['table_name'].unique().tolist()
                for tbl in tbls:
                    query_list = []
                    # get the list of rules at table level and perform iteration
                    rules = dq_rule[(dq_rule['source_system'] == src) &
                                    (dq_rule['database_name'] == db) & 
                                    (dq_rule['table_name'] == tbl)][rules_subset]
                    rules['params'] = rules['params'].fillna('')
                    meta_data = df_metadata[(df_metadata['src_name'] == src) &
                                            (df_metadata['db_name'] == db) & 
                                            (df_metadata['tbl_name'] == tbl)][['col_name', 'col_id', 'data_type']]
                    for rule in rules.itertuples():
                        rule_id = df_rule_metadata[df_rule_metadata['rule_name'] == rule.rule_name]['rule_id'].values[0]
                        api_call = dq_apis[dq_apis['api_template'] == rule.api_template]['api_call'].values[0]
                        # collect the columns in scope
                        col_list = []
                        if rule.rule_scope == 'table':
                            col_list += list(zip(meta_data['col_name'],meta_data['col_id']))
                        else:
                            col_list += [(rule.column_name, \
                                          meta_data[meta_data['col_name'] == rule.column_name]['col_id'].values[0])]
                        for col_name, col_id in col_list:
                            args_dict = {'spark':'spark', 'db':'db','tbl':'tbl','col':'col_name'}
                            if rule.params != '':
                                param_dict = rule.params[0].copy()
                                args_dict.update(param_dict)
                            args_list = []
                            for key, val in args_dict.items():
                                args_list += [f'{key}={val}']
                            api_call_string = f"dq.{api_call}({','.join(args_list)})"
                            print(f'API Call: {api_call_string}')
                            dq_val = eval(api_call_string)
                            result_dict += [{'col_id': str(col_id), 'rule_id':str(rule_id), 'dq_value':dq_val}]
        result = pd.DataFrame(result_dict)[['col_id', 'rule_id', 'dq_value']]
        api_results = pd.concat([api_results, result])
        print(f'{len(dq_rule)} APIs are executed')
    return api_results

# COMMAND ----------

# DBTITLE 1,Load Results to Metrics Table
def load_metrics(spark, pd, sql_results, sql_dq_failed, api_results, config):
    """
    Function to load the data quality validation results to metrics table
    
    Args
    ----
    spark: Name of the spark session
    pd: Alias name of the padas library
    sql_results: Name of sql results dataframe
    sql_dq_failed: List of dq check failed dataframes
    api_results: Name of the api results dataframe
    config: Name of dq process config json object
    """
    metrics_db = config['dq_metrics']['dq_metrics_db']
    meta_tbl = config['dq_metrics']['dq_meta_tbl']
    meta_col = config['dq_metrics']['dq_meta_col']
    metrics = config['dq_metrics']['dq_metrics']
    failed_rec = config['dq_metrics']['dq_check_failed']
    
    ###############################################
    # Load data quality metrics
    ###############################################
    # create view with results dataframe
    results = pd.concat([sql_results, api_results])
    spark.createDataFrame(results).createOrReplaceTempView('dq_results_vw')
    print('Load Metrics Table:')
    # delete stats in case of rerun on the same day
    df_del = spark.sql(f'''
    DELETE FROM {metrics_db}.{metrics} tgt
    WHERE EXISTS (
    SELECT rule_id, col_id FROM dq_results_vw stg
    WHERE tgt.rule_id = stg.rule_id
    AND tgt.col_id = stg.col_id
    ) AND tgt.created_on = current_date''')
    # load stats to metrics table
    df_ins = spark.sql(f'''
    INSERT INTO {metrics_db}.{metrics} 
    (rule_id, col_id, dq_processed_cnt, dq_passed_cnt, created_on)
    SELECT 
    a.rule_id AS rule_id,
    a.col_id AS col_id,
    c.curr_rec_cnt AS dq_processed_cnt, 
    a.dq_value AS dq_passed_cnt,
    current_date() as created_on
    FROM 
    dq_results_vw a
    INNER JOIN {metrics_db}.{meta_col} b ON a.col_id = b.col_id
    INNER JOIN {metrics_db}.{meta_tbl} c ON b.tbl_id = c.tbl_id
    ''')
    # Summary
    del_status = df_del.collect()[0]
    ins_status = df_ins.collect()[0]
    print(f'''Table: {metrics}, Delete:{del_status['num_affected_rows']}, Insert: {ins_status['num_inserted_rows']}''')

    ###############################################
    # Load DQ Check Failed Records
    ###############################################
    # define temporary table to capture the failed records
    spark.sql(f'''DROP TABLE IF EXISTS {metrics_db}.tmp_dq_failed''')
    spark.sql(f'''
        create table {metrics_db}.tmp_dq_failed(
            col_id bigint,
            rule_id bigint,
            failed_rec string)''')
    # Helper function
    def union_all(*dfs):
        '''Helper function to union spark dataframes'''
        return reduce(DataFrame.union, dfs)
    # Create view with failed records
    if len(sql_dq_failed) > 0:
        final_df = union_all(*sql_dq_failed.values())
        final_df = final_df.repartition(50)
        split_col = F.split(final_df['col_rule'], '_')
        final_df = final_df.withColumn('col_id', split_col.getItem(0))\
                        .withColumn('rule_id', split_col.getItem(1))\
                        .select('col_id', 'rule_id', 'primary_key')
        final_df.createOrReplaceTempView('dq_failed_vw')
        print('Load Failed Record Table:')
        # delete stats in case of rerun on the same day
        df_fail_del = spark.sql(f'''
        DELETE FROM {metrics_db}.{failed_rec} tgt
        WHERE EXISTS (
        SELECT rule_id, col_id FROM dq_failed_vw stg
        WHERE tgt.rule_id = stg.rule_id
        AND tgt.col_id = stg.col_id
        ) AND tgt.created_on = current_date''')
        # load stats to metrics table
        df_fail_ins = spark.sql(f'''
        INSERT INTO {metrics_db}.{failed_rec} 
        (rule_id, col_id, failed_rec, created_on)
        SELECT 
        a.rule_id AS rule_id,
        a.col_id AS col_id,
        a.primary_key AS failed_rec,
        current_date() as created_on
        FROM 
        dq_failed_vw a
        ''')
        # Summary
        del_status = df_fail_del.collect()[0]
        ins_status = df_fail_ins.collect()[0]
        print(f'''Table: {failed_rec}, Delete:{del_status['num_affected_rows']}, Insert: {ins_status['num_inserted_rows']}''')
    return

# COMMAND ----------

# DBTITLE 1,Data Quality Engine
def dq_engine(spark, pd, src_sys, config_path, config_file):
    """
    Main function to execute automated data quality checks. 
    
    Args
    ----
    spark: Name of the spark session
    pd: Alias name of the pandas library
    src_sys : Name of the source system
    config_path: Path to configuration directory
    config_file: Name of the dq process config gile
    """
    
    # execute dq checks
    try:
        
        print('DQ Process started')
        # execute rules parser
        dq_rule, dq_templates, dq_queries, dq_apis, df_metadata, df_rule_metadata, config \
                        = rules_parser(spark, pd, src_sys, config_path, config_file)
        
        # execute query generator
        dq_metric_queries,track_failed_rules = query_genarator(dq_rule, dq_templates, dq_queries, df_metadata, df_rule_metadata)
        
        # execute queries
        sql_results,sql_dq_failed = query_executor(spark, pd, dq_metric_queries,track_failed_rules)
        
        # execute APIs
        api_results = api_executor(spark, pd, dq_rule, dq_apis, df_metadata, df_rule_metadata)
        
        # load results to metrics table
        load_metrics(spark, pd, sql_results, sql_dq_failed ,api_results, config)
        
        print(f'DQ Process completed')
    
    except Exception as e:
        print('DQ Process Failed')
        raise
    return dq_metric_queries

# COMMAND ----------

queries = dq_engine(spark, pd, src_sys, config_path, config_file)

# COMMAND ----------

# DBTITLE 1,Data Quality Report
# MAGIC %sql
# MAGIC select 
# MAGIC d.db_name as `DB Name`,
# MAGIC c.tbl_name as `Table Name`,
# MAGIC b.col_name as `Column Name`,
# MAGIC regexp_replace(e.rule_name, '_',' ' ) as `DQ Rule`,
# MAGIC initcap(e.dq_dim_name) as `DQ Dimension`,
# MAGIC a.dq_passed_cnt as `DQ Passed Rows`,
# MAGIC a.dq_processed_cnt - a.dq_passed_cnt as `DQ Failed Rows`,
# MAGIC (a.dq_passed_cnt/a.dq_processed_cnt) * 100 as `DQ Percentage`
# MAGIC from 
# MAGIC dqmetrics.dq_metrics a 
# MAGIC INNER JOIN dqmetrics.dq_col_metadata b ON a.col_id = b.col_id
# MAGIC INNER JOIN dqmetrics.dq_tbl_metadata c ON b.tbl_id = c.tbl_id
# MAGIC INNER JOIN dqmetrics.dq_db_metadata d ON c.db_id = d.db_id
# MAGIC INNER JOIN dqmetrics.dq_rules_metadata e ON a.rule_id = e.rule_id 
# MAGIC WHERE a.created_on = current_date() order by 1,2,5,4,3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC d.db_name as `DB Name`,
# MAGIC c.tbl_name as `Table Name`,
# MAGIC b.col_name as `Column Name`,
# MAGIC regexp_replace(e.rule_name, '_',' ' ) as `Failed DQ Rule`,
# MAGIC a.failed_rec as `Failed DQ Record`
# MAGIC from 
# MAGIC dqmetrics.dq_check_failed a 
# MAGIC INNER JOIN dqmetrics.dq_col_metadata b ON a.col_id = b.col_id
# MAGIC INNER JOIN dqmetrics.dq_tbl_metadata c ON b.tbl_id = c.tbl_id
# MAGIC INNER JOIN dqmetrics.dq_db_metadata d ON c.db_id = d.db_id
# MAGIC INNER JOIN dqmetrics.dq_rules_metadata e ON a.rule_id = e.rule_id 
# MAGIC WHERE a.created_on = current_date() order by 1,2,5,4,3;
