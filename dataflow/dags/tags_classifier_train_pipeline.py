import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from dataflow.dags import _PipelineDAG
from dataflow.utils import TableConfig
from dataflow.dags.dataset_pipelines import InteractionsDatasetPipeline
from dataflow.operators.tags_classifier.train.train import (
    model_training_with_labelled_data,
)

class TagsClassifierTrainPipelineRefactor(_PipelineDAG):
    table_config = TableConfig(
        table_name='interactions_tags_classifier_metrics',
        field_mapping=[
            ("model_version", sa.Column("model_version", sa.Text)),
            ("model_for_tag", sa.Column("model_for_tag", sa.Text)),
            ("size", sa.Column("size", sa.Integer)),
            ("precisions", sa.Column("precisions", sa.Numeric)),
            ("recalls", sa.Column("recalls", sa.Numeric)),
            ("f1", sa.Column("f1", sa.Numeric)),
            ("accuracy", sa.Column("accuracy", sa.Numeric)),
            ("auc", sa.Column("auc", sa.Numeric)),
        ],
    )

    controller_pipeline = InteractionsDatasetPipeline
    # dependencies = [InteractionsDatasetPipeline]

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='train-model',
            python_callable=model_training_with_labelled_data,
            queue='tensorflow',
            provide_context=True,
            # op_args=[self.target_db,  self.table_config.table_name],
            op_args=[self.table_config.table_name],
        )
