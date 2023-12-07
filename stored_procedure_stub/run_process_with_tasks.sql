 CREATE OR REPLACE PROCEDURE run_process_with_tasks(process_name STRING, vars STRING, execute_flag STRING)
  RETURNS VARIANT
  LANGUAGE PYTHON
--   RUNTIME_VERSION = '3.8'
--   PACKAGES = ('snowflake-snowpark-python==0.10.0','Jinja2==3.1.2', 'colorama==0.4.6')
  RUNTIME_VERSION = '3.10'
  PACKAGES = ('snowflake-snowpark-python==1.5.1','Jinja2==3.1.2', 'colorama==0.4.6')
  IMPORTS = ('@tips/tips.zip')
  HANDLER = 'tips.framework.app.runProcessWithTasks'
  EXECUTE AS CALLER;