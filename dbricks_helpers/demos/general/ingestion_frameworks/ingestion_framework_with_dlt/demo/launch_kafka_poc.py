import uuid
from src.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)
import traceback
from databricks.sdk.service import jobs, compute
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary, PipelineCluster


class KafkaPOCRunner(DLTMETARunner):

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: DLTMetaRunnerConf):
        """
        Runs the Kafka POC by calling the necessary methods in the correct order.
        """
        try:
            self.init_dltmeta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()

    def init_runner_conf(self) -> DLTMetaRunnerConf:
        """
        Initialize the runner configuration for Kafka POC.
        """
        run_id = uuid.uuid4().hex
        runner_conf = DLTMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            uc_catalog_name=self.args["uc_catalog_name"],
            int_tests_dir="demo",
            dlt_meta_schema="gap_kafka_dataflowspecs",
            bronze_schema="gap_kafka_bronze",
            runners_nb_path=f"/Users/{self.wsi._my_username}/kafka_poc/gap_kafka_src1",
            source="kafka",
            kafka_template="demo/conf/kafka-poc-onboarding.template",
            kafka_source_topic="jomin_johny_fe_tech_onboarding_kafka_test-3",
            kafka_source_servers_secrets_scope_name="oetrta",
            kafka_source_servers_secrets_scope_key="kafka-bootstrap-servers-plaintext",
            env="demo",
            onboarding_file_path="demo/conf/onboarding.json",
            runners_full_local_path='./demo/notebooks/kafka_poc_runners/',
            test_output_file_path=(
                f"/Users/{self.wsi._my_username}/kafka_poc/"
                f"{run_id}/kafka-poc-output.csv"
            ),
        )

        return runner_conf

    def create_dlt_meta_pipeline(
        self,
        pipeline_name: str,
        layer: str,
        group: str,
        target_schema: str,
        runner_conf: DLTMetaRunnerConf,
    ) -> str:
        """Create DLT pipeline in TRIGGERED mode - runs once then stops"""
        configuration = {
            "layer": layer,
            f"{layer}.group": group,
            "dlt_meta_whl": runner_conf.remote_whl_path,
            "pipelines.externalSink.enabled": "true",
        }
        
        configuration[f"{layer}.dataflowspecTable"] = (
            f"{runner_conf.uc_catalog_name}.{runner_conf.dlt_meta_schema}.{layer}_dataflowspec_cdc"
        )
        
        created = self.ws.pipelines.create(
            catalog=runner_conf.uc_catalog_name,
            name=pipeline_name,
            serverless=False,  # Use classic compute for network connectivity
            continuous=False,  # Triggered mode - runs once then stops
            configuration=configuration,
            clusters=[
                PipelineCluster(
                    label="default",
                    num_workers=1
                )
            ],
            libraries=[
                PipelineLibrary(
                    notebook=NotebookLibrary(
                        path=f"{runner_conf.runners_nb_path}/runners/init_dlt_meta_pipeline.py"
                    )
                )
            ],
            schema=target_schema,
        )

        if created is None:
            raise Exception("Pipeline creation failed")
        return created.pipeline_id

    def create_workflow_spec(self, runner_conf: DLTMetaRunnerConf):
        """Create a simplified workflow for Kafka POC - no publish_events task needed"""
        dltmeta_environments = [
            jobs.JobEnvironment(
                environment_key="dl_meta_kafka_poc_env",
                spec=compute.Environment(
                    client="1",
                    dependencies=[runner_conf.remote_whl_path],
                ),
            )
        ]
        tasks = [
            jobs.Task(
                task_key="setup_dlt_meta_pipeline_spec",
                environment_key="dl_meta_kafka_poc_env",
                description="Setup DLT Meta Pipeline Spec",
                timeout_seconds=0,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="dlt_meta",
                    entry_point="run",
                    named_parameters={
                        "onboard_layer": "bronze",
                        "database": f"{runner_conf.uc_catalog_name}.{runner_conf.dlt_meta_schema}",
                        "onboarding_file_path": f"{runner_conf.uc_volume_path}/{self.base_dir}/conf/onboarding.json",
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "import_author": "Kafka_POC",
                        "version": "v1",
                        "bronze_dataflowspec_path": f"{runner_conf.uc_volume_path}/data/dlt_spec/bronze",
                        "overwrite": "True",
                        "env": runner_conf.env,
                        "uc_enabled": "True",
                    },
                ),
            ),
            jobs.Task(
                task_key="bronze_dlt_pipeline",
                depends_on=[
                    jobs.TaskDependency(task_key="setup_dlt_meta_pipeline_spec")
                ],
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
        ]

        return self.ws.jobs.create(
            name="gap_kafka_src1_bronze",
            environments=dltmeta_environments,
            tasks=tasks,
        )

    def launch_workflow(self, runner_conf: DLTMetaRunnerConf):
        created_job = self.create_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    kafka_poc_runner = KafkaPOCRunner(args, workspace_client, "demo")
    print("Initializing Kafka POC...")
    runner_conf = kafka_poc_runner.init_runner_conf()
    kafka_poc_runner.run(runner_conf)


if __name__ == "__main__":
    main()