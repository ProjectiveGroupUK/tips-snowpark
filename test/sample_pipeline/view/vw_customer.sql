create or replace view tips_test_transform.vw_customer(
	C_CUSTKEY,
	C_NAME,
	C_ADDRESS,
	C_ADDRESS_ORIG,
	C_NATIONKEY,
	C_PHONE,
	C_ACCTBAL,
	C_MKTSEGMENT,
	C_COMMENT
) as
  SELECT C_CUSTKEY,C_NAME, CASE WHEN (row_number() OVER (ORDER BY 1)) % 1000 = 1 THEN randstr(10, random()) ELSE C_ADDRESS END c_address, c_address c_address_orig ,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT 
  FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;