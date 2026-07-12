from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time
from airflow.models import Variable


@dag
def orchestrate():

    @task
    def ingest_cdc():
        # Logic to ingest CDC data
        ws = WorkspaceClient(
            host=Variable.get("DATABRICKS_HOST"),
            token=Variable.get("DATABRICKS_TOKEN")
        )

        job_trigger = ws.jobs.run_now(job_id=109246969235953)

        while True:

            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break 
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
                    
            time.sleep(5)  # Wait for 5 seconds before checking the job status again
        
        return "CDC Ingestion Completed"
    

    @task.bash
    def cleanup():
        # Logic to clean up resources
        return "rm -rf /opt/airflow/walmart_project/walmart_project/target && rm -rf /opt/airflow/walmart_project/walmart_project/logs"

    @task.bash
    def source_freshness():
        # manually setting the working directory to the project folder
        return "cd /opt/airflow/walmart_project/walmart_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd = "/opt/airflow/walmart_project/walmart_project",
        bash_command="dbt run --select silver_t"
    )

    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",
        cwd = "/opt/airflow/walmart_project/walmart_project",
        bash_command="dbt test --select silver_t"
    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd = "/opt/airflow/walmart_project/walmart_project",
        bash_command="dbt run --select silver_b"
    )

    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        cwd = "/opt/airflow/walmart_project/walmart_project",
        bash_command="dbt test --select silver_b"
    )

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd = "/opt/airflow/walmart_project/walmart_project",
        bash_command="dbt run --select gold/ephemeral"
    )

    gold_dimensional = BashOperator(
        task_id="gold_dimensional",
        cwd = "/opt/airflow/walmart_project/walmart_project",
        bash_command="dbt snapshot"
    )

    gold_fact = BashOperator(
        task_id="gold_fact",
        cwd = "/opt/airflow/walmart_project/walmart_project",
        bash_command="dbt run --select gold/fact"
    )



    ingest_cdc() >> cleanup() >> source_freshness() >> silver_technical >> silver_technical_tests>> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensional >> gold_fact

orchestrate_dag = orchestrate()
