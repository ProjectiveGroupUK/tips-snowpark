 CREATE OR REPLACE PROCEDURE run_process_sp(process_name STRING, vars STRING, execute_flag STRING)
  RETURNS VARIANT
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python','Jinja2','PyYAML', 'colorama')
  IMPORTS = ('@tips/tips.zip')
  HANDLER = 'tips.framework.app.run';