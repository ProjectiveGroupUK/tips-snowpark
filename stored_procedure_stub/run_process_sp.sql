 CREATE OR REPLACE PROCEDURE run_process_sp(process_name STRING, vars STRING, execute_flag STRING)
  RETURNS VARIANT
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python==0.10.0','Jinja2==3.1.2', 'colorama==0.4.6')
  IMPORTS = ('@tips/tips.zip')
  HANDLER = 'tips.framework.app.run';