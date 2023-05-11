create or replace view tips_test_transform.vw_src_customer_history(
	CUSTOMER_NAME,
	CUSTOMER_ADDRESS,
	CUSTOMER_PHONE,
	CUSTOMER_MARKET_SEGMENT,
	CUSTOMER_COMMENT,
	COUNTRY,
	REGION,
	NATION_KEY,
	REGION_KEY,
	EFFECTIVE_START_DATE
) as
  SELECT
	c_name CUSTOMER_NAME,
	C_ADDRESS customer_address ,
	C_PHONE customer_phone,
	C_MKTSEGMENT customer_market_segment,
	C_COMMENT customer_comment,
	n_name country,
	r_name region,
	C_NATIONKEY nation_key,
	r_region_key region_key,
	to_date(to_char(cobid),'YYYYMMDD') effective_start_date
FROM
	tips_test_transform.customer_with_lookups;