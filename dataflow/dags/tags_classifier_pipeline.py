from dataflow.operators.tags_classifier_model.tags_classifier import make_prediction
import sqlalchemy as sa
from airflow.operators.python_operator import PythonOperator
from dataflow.dags import _PipelineDAG
from dataflow.utils import TableConfig
from dataflow.dags.dataset_pipelines import InteractionsDatasetPipeline
import datetime


class TagsClassifierPipeline(_PipelineDAG):
    table_config = TableConfig(
        table_name='interactions_dataset_with_tags',
        field_mapping=[
            ("id", sa.Column("id", sa.Text, primary_key=True)),
            ("policy_feedback_notes", sa.Column("policy_feedback_notes", sa.Text)),
            ("tags_prediction", sa.Column("tags_prediction", sa.Text)),
            ("tag_1", sa.Column("tag_1", sa.Text)),
            ("probability_score_1", sa.Column("probability_score_1", sa.Numeric)),
            ("tag_2", sa.Column("tag_2", sa.Text)),
            ("probability_score_2", sa.Column("probability_score_2", sa.Numeric)),
            ("tag_3", sa.Column("tag_3", sa.Text)),
            ("probability_score_3", sa.Column("probability_score_3", sa.Numeric)),
            ("tag_4", sa.Column("tag_4", sa.Text)),
            ("probability_score_4", sa.Column("probability_score_4", sa.Numeric)),
            ("tag_5", sa.Column("tag_5", sa.Text)),
            ("probability_score_5", sa.Column("probability_score_5", sa.Numeric)),
        ],
    )

    controller_pipeline = InteractionsDatasetPipeline
    # todo - uncomment this when testing is done
    # dependencies = [InteractionsDatasetPipeline]

    today = datetime.date.today()
    six_week_ago = today - datetime.timedelta(weeks=6)

    # todo - add 6 weeks' data only (check the downstream query)
    query = f"""
             SELECT id, policy_feedback_notes  FROM "public"."{controller_pipeline.table_config.table_name}"
                          where policy_feedback_notes!='' limit 15000
             """
    # and modified_on >= '{six_week_ago}'
    # limit 15000

    def get_fetch_operator(self) -> PythonOperator:
        return PythonOperator(
            task_id='make-prediction',
            python_callable=make_prediction,
            queue='tensorflow',
            provide_context=True,
            op_args=[self.target_db, self.query, self.table_config.table_name],
        )
