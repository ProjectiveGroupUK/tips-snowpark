--Delete existing records for TIPS_TEST_PIPELINE, if any
DELETE 
  FROM process_cmd 
 WHERE process_id = (SELECT process_id 
                       FROM process
					  WHERE process_name = 'TIPS_TEST_PIPELINE')
;
					  
DELETE 
  FROM process
 WHERE process_name = 'TIPS_TEST_PIPELINE'
;

--Now Insert Records

--Add records in process table
INSERT INTO process (process_name, process_description)
VALUES ('TIPS_TEST_PIPELINE','This is a dummy pipeline to Test TiPS execution for the first time after setup to make sure TiPS is working');

SET process_id = (SELECT process_id FROM process WHERE process_name = 'TIPS_TEST_PIPELINE');

--Add records in process_cmd table
INSERT INTO process_cmd 
(	
	process_id
,   process_cmd_id
,	cmd_type
,	cmd_src
,	cmd_tgt
,	cmd_where
,	cmd_binds
,	refresh_type
,	business_key
,	merge_on_fields
,	generate_merge_matched_clause
,	generate_merge_non_matched_clause
,	additional_fields
,	temp_table
,	cmd_pivot_by
,	cmd_pivot_field
,	dq_type
)	
VALUES 
(	
	$process_id 												--process_id
,   10															--process_cmd_id
,	'REFRESH'													--cmd_type
,	'TIPS_TEST_TRANSFORM.VW_CUSTOMER'							--cmd_src
,	'TIPS_TEST_TRANSFORM.CUSTOMER'								--cmd_tgt
,	'C_MKTSEGMENT = :2 AND COBID = :1'							--cmd_where
,	'COBID|MARKET_SEGMENT'										--cmd_binds
,	'DI'														--refresh_type
,	NULL														--business_key
,	NULL														--merge_on_fields
,	NULL														--generate_merge_matched_clause
,	NULL														--generate_merge_non_matched_clause
,	'TO_NUMBER(:1) COBID'										--additional_fields
,	NULL														--temp_table
,	NULL														--cmd_pivot_by
,	NULL														--cmd_pivot_field
,	NULL														--dq_type
),
(	
	$process_id													--process_id
,   20															--process_cmd_id
,	'APPEND'													--cmd_type
,	'TIPS_TEST_TRANSFORM.VW_CUSTOMER_WITH_LOOKUPS'				--cmd_src
,	'TIPS_TEST_TRANSFORM.CUSTOMER_WITH_LOOKUPS'					--cmd_tgt
,	'C_MKTSEGMENT = :2 AND COBID = :1'							--cmd_where
,	'COBID|MARKET_SEGMENT'										--cmd_binds
,	NULL														--refresh_type
,	NULL														--business_key
,	NULL														--merge_on_fields
,	NULL														--generate_merge_matched_clause
,	NULL														--generate_merge_non_matched_clause
,	NULL														--additional_fields
,	'Y'															--temp_table
,	NULL														--cmd_pivot_by
,	NULL														--cmd_pivot_field
,	NULL														--dq_type
),
(	
	$process_id													--process_id
,   30															--process_cmd_id
,	'PUBLISH_SCD2_DIM'											--cmd_type
,	'TIPS_TEST_TRANSFORM.VW_SRC_CUSTOMER_HISTORY'				--cmd_src
,	'TIPS_TEST_DIMENSION.CUSTOMER_HISTORY'						--cmd_tgt
,	NULL														--cmd_where
,	'COBID'														--cmd_binds
,	NULL														--refresh_type
,	'CUSTOMER_NAME'												--business_key
,	NULL														--merge_on_fields
,	NULL														--generate_merge_matched_clause
,	NULL														--generate_merge_non_matched_clause
,	NULL														--additional_fields
,	NULL														--temp_table
,	NULL														--cmd_pivot_by
,	NULL														--cmd_pivot_field
,	'SCD2'														--dq_type
),
(	
	$process_id													--process_id
,   40															--process_cmd_id
,	'MERGE'														--cmd_type
,	'TIPS_TEST_TRANSFORM.VW_SRC_CUSTOMER'						--cmd_src
,	'TIPS_TEST_DIMENSION.CUSTOMER'								--cmd_tgt
,	NULL														--cmd_where
,	NULL														--cmd_binds
,	NULL														--refresh_type
,	'CUSTOMER_NAME'												--business_key
,	'CUSTOMER_NAME'												--merge_on_fields
,	'Y'															--generate_merge_matched_clause
,	'Y'															--generate_merge_non_matched_clause
,	NULL														--additional_fields
,	NULL														--temp_table
,	NULL														--cmd_pivot_by
,	NULL														--cmd_pivot_field
,	'DUPS'														--dq_type
)
;
