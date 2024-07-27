-- Create database
create database dqmetrics location '';

-- Create Metadata tables
create table dqmetrics.dq_db_metadata(
db_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
db_name string,
src_name string,
db_catalog string,
db_desc string,
db_loc string,
created_on date,
last_updated_on date
) 
using delta

create table dqmetrics.dq_tbl_metadata(
tbl_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
db_id bigint,
tbl_name string,
tbl_desc string,
tbl_type string,
is_temp string,
curr_rec_cnt int,
curr_col_cnt int,
created_on date,
last_updated_on date
) 
using delta

create table dqmetrics.dq_col_metadata(
col_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
tbl_id bigint,
col_name string,
col_desc string,
data_type string,
is_nullable string,
is_partition string,
is_bucket string,
created_on date,
last_updated_on date
) 
using delta

create table dqmetrics.dq_rules_metadata(
rule_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
rule_name string,
dq_dim_name string,
created_on date,
last_updated_on date
) 
using delt

create table dqmetrics.dq_metrics(
metric_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
rule_id bigint,
col_id bigint,
dq_processed_cnt bigint,
dq_passed_cnt bigint,
created_on date
) 
using delta

create table dqmetrics.dq_check_failed(
col_id bigint,
rule_id bigint,
failed_rec string,
created_on date
) 
using delta

