import json
import os
import zipfile
import numpy as np
from itertools import compress
from dataflow.operators.tags_classifier.utils import preprocess
from dataflow.operators.tags_classifier.setting import (
    MAX_SEQUENCE_LENGTH,
    model_version,
    tags_covid,
    tags_general,
)
import operator
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from dataflow import config
from dataflow.utils import logger, S3Data
from airflow.hooks.S3_hook import S3Hook
from tempfile import TemporaryDirectory


def transform_X(X_text, tokenizer):
    from tensorflow.keras.preprocessing import sequence

    X = tokenizer.texts_to_sequences(X_text)
    X = sequence.pad_sequences(
        X, maxlen=MAX_SEQUENCE_LENGTH, padding='post', truncating='post'
    )
    return X


def fetch_model():
    bucket = config.S3_IMPORT_DATA_BUCKET
    logger.info(f"Bucket: {bucket}")

    client = S3Hook("DEFAULT_S3")

    s3_key_object = client.get_key(
        'models/data_hub_policy_feedback_tags_classifier/' + model_version,
        bucket_name=bucket,
    )

    # note: 'wb' rather than 'w'
    with open(model_version, 'wb') as data:
        s3_key_object.download_fileobj(data)

    with zipfile.ZipFile(model_version, "r") as zip_ref:
        # it extracts directly to models_general and models_covid
        zip_ref.extractall()
        logger.info(f"working dir: {os.getcwd()}")
        logger.info(f"list contents: {os.listdir()}")


def _predict(X_to_predict, tokenizer, tags_to_predict, model_path):
    import tensorflow as tf

    logger.info("Start making prediction")

    ids = X_to_predict['id']
    X_to_predict = X_to_predict['sentence']
    text_to_predict = X_to_predict.copy()

    X_to_predict = transform_X(X_to_predict.values, tokenizer)
    Y_test_predict = np.zeros((X_to_predict.shape[0], len(tags_to_predict)))
    Y_test_predict_prob = np.zeros((X_to_predict.shape[0], len(tags_to_predict)))

    for ind, tag_i in enumerate(['_'.join(j.split(' ')) for j in tags_to_predict]):
        logger.info(f"Predicting for tag {ind}, {tag_i}")
        m = tf.keras.models.load_model(model_path + tag_i)
        test_predictions_prob_tag = m.predict(X_to_predict)
        test_predictions_class_tag = (test_predictions_prob_tag > 0.5) + 0
        Y_test_predict_prob[:, ind] = np.concatenate(test_predictions_prob_tag)
        Y_test_predict[:, ind] = np.concatenate(test_predictions_class_tag)

    predict = []
    sentence = []
    predict_prob = []

    for i in np.arange(0, X_to_predict.shape[0]):
        sentence.append(X_to_predict[i])
        predict.append(list(compress(tags_to_predict, Y_test_predict_prob[i] > 0.5)))
        predict_prob.append(dict(zip(tags_to_predict, Y_test_predict_prob[i])))

    prediction_on_data = pd.DataFrame(
        {
            'id': ids,
            'sentence': text_to_predict,
            'prediction': predict,
            'prediction_prob': predict_prob,
        }
    )

    return prediction_on_data


def update_prediction_dict(d1, d2):
    d = d1.copy()
    if not isinstance(d1, float) and not isinstance(d2, float):
        d.update(d2)
    return d


def top_5_labels(dict_of_probs, threshold=0.5):
    dict_of_probs_final = dict_of_probs.copy()
    dict_of_probs_final = {
        k: np.round(v, 2) for k, v in dict_of_probs_final.items() if v > threshold
    }
    covid_tags = [
        i for i in dict_of_probs_final.keys() if i.lower().startswith('covid')
    ]
    if len(covid_tags) == 1:
        dict_of_probs_final = {
            k.replace('Covid-19', 'Covid-19 General'): v
            for k, v in dict_of_probs_final.items()
        }
    elif len(covid_tags) > 1:
        dict_of_probs_final.pop('Covid-19', None)
    dict_of_probs_final = dict(
        sorted(dict_of_probs_final.items(), key=operator.itemgetter(1), reverse=True)[
            :5
        ]
    )
    if isinstance(dict_of_probs_final, float) or len(dict_of_probs_final) == 0:
        dict_of_probs_final = {'General': 0}
    return dict_of_probs_final


def write_prediction(table_name, df, context):
    # note: if making this to a separate task, then **context need to be passed to the function and
    # df = context['task_instance'].xcom_pull(task_ids='predict-tags')
    df_json = df.to_json(orient="records")
    df_json = json.loads(df_json)

    s3 = S3Data(table_name, context["ts_nodash"])
    s3.write_key('tags_prediction.json', df_json)


def make_prediction(target_db: str, query: str, table_name, **context):

    with TemporaryDirectory() as tempdir:
        os.chdir(tempdir)
        os.mkdir('the_models')
        os.chdir(tempdir + '/the_models')
        logger.info(f"working dir: {os.getcwd()}")

        logger.info("step 1: fetch model")
        fetch_model()

        logger.info("step 2: fetch data")
        df = fetch_interaction_data(target_db, query)

        logger.info("step 3: make prediction")
        predictions = predict_tags(df)

        logger.info("step 4: write prediction to S3")
        write_prediction(table_name, predictions, context)


def predict_tags(df):
    # note: if making this to a separate task, then **context need to be passed to the function and
    # df = context['task_instance'].xcom_pull(task_ids='fetch-interaction-data')

    logger.info(f"check working dir: {os.getcwd()}")
    logger.info(f"check general models: {os.listdir('models_general')}")
    logger.info(f"check covid models: {os.listdir('models_covid')}")

    from keras_preprocessing.text import tokenizer_from_json

    with open('models_general/cnn_tokenizer.json') as f:
        data_general = json.load(f)
        tokenizer_general = tokenizer_from_json(data_general)

    with open('models_covid/cnn_tokenizer.json') as f:
        data_covid = json.load(f)
        tokenizer_covid = tokenizer_from_json(data_covid)

    prediction_on_general_data = _predict(
        X_to_predict=df,
        tokenizer=tokenizer_general,
        tags_to_predict=tags_general,
        model_path='models_general/',
    )

    covid_to_predict = prediction_on_general_data[
        prediction_on_general_data['prediction'].apply(lambda x: 'Covid-19' in x)
    ]
    logger.info(f"check covid df shape: {covid_to_predict.shape[0]}")

    if covid_to_predict.shape[0] > 0:

        prediction_on_covid_data = _predict(
            X_to_predict=covid_to_predict[['id', 'sentence']],
            tokenizer=tokenizer_covid,
            tags_to_predict=tags_covid,
            model_path='models_covid/',
        )

        predictions = prediction_on_general_data.merge(
            prediction_on_covid_data, left_on='id', right_on='id', how='left'
        )
        predictions['prediction_prob'] = predictions.apply(
            lambda x: update_prediction_dict(
                x['prediction_prob_x'], x['prediction_prob_y']
            ),
            axis=1,
        )

    else:
        predictions = prediction_on_general_data.copy()

    predictions['prediction_prob_top_5'] = predictions.apply(
        lambda x: top_5_labels(x['prediction_prob'], threshold=0.5), axis=1
    )
    predictions['tags_prediction'] = predictions.apply(
        lambda x: list(x['prediction_prob_top_5'].keys()), axis=1
    )
    predictions['tags_prediction'] = predictions['tags_prediction'].apply(
        lambda x: ','.join(x)
    )

    if 'sentence_y' in predictions.columns:
        del predictions['sentence_y']
    predictions = predictions.rename(columns={'sentence_x': 'sentence'})

    # memory_used = memory_profiler.memory_usage()
    # print('memory used', memory_used[0] - memory_used0[0])

    predictions = predictions[
        ['id', 'sentence', 'tags_prediction', 'prediction_prob_top_5']
    ]
    predictions = predictions.rename(columns={'sentence': 'policy_feedback_notes'})

    predictions = update_df_column_with_prob(predictions)
    # if 'prediction_prob_top_5' in predictions.columns:
    #     del predictions['prediction_prob_top_5']

    # predictions.to_csv('to_predict_result.csv', index=False)
    return predictions


def convert_prob_dict(a_dict):
    a_dict = {k: v for k, v in sorted(a_dict.items(), key=lambda item: item[1] * -1)}
    converted_dict = {}
    for i, (k, v) in enumerate(a_dict.items()):
        converted_dict['tag_' + str(i + 1)] = k
        converted_dict['probability_score_tag_' + str(i + 1)] = v
    for i in range(1, 6):
        if 'tag_' + str(i) not in converted_dict:
            converted_dict['tag_' + str(i)] = None
            converted_dict['probability_score_tag_' + str(i)] = None
    return converted_dict


def update_df_column_with_prob(df):
    df['prediction_prob_top_5_converted'] = df['prediction_prob_top_5'].apply(
        lambda x: convert_prob_dict(x)
    )
    for i in range(1, 6):
        df['tag_' + str(i)] = df['prediction_prob_top_5_converted'].apply(
            lambda x: x['tag_' + str(i)]
        )
        df['probability_score_tag_' + str(i)] = df[
            'prediction_prob_top_5_converted'
        ].apply(lambda x: x['probability_score_tag_' + str(i)], 2)
    del df['prediction_prob_top_5_converted']
    del df['prediction_prob_top_5']
    return df


def fetch_interaction_data(target_db: str, query: str):
    logger.info("starting fetching data")

    try:
        # create connection with named cursor to fetch data in batches
        connection = PostgresHook(postgres_conn_id=target_db).get_conn()
        cursor = connection.cursor(name='fetch_interaction')
        cursor.execute(query)

        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        df.columns = [column[0] for column in cursor.description]
        df = preprocess(df, action='predict')
        # df.to_csv('to_predict.csv', index=False)

        logger.info(f"check df shape: {df.shape}")
        logger.info(f"df head: {df.head()}")

    finally:
        if connection:
            cursor.close()
            connection.close()

    return df


# # run this to be able to check data in python console when test locally
# df_a = pd.read_csv('/Users/linglingli/DIT/data-flow/prediction_data_0904.csv')
# df_a = preprocess(df_a, action='predict')
## os.chdir('/Users/linglingli/DIT/data-flow/the_models/models_20201029')
# predictions_a = predict_tags(df_a)
# predictions_a['tags_prediction']

# # test one sentence
# test = [['id1', """Company has had to make one of their valued long-term designers redundant in the last month during pandemic.
# They had no other choice but this is a very difficult environment for a creative company to be working in."""]]
# test = [['id1', """Company has had to make one of their valued long-term designers redundant in the last month
# They had no other choice but this is a very difficult environment for a creative company to be working in."""]]
# # # todo: long paragraph, lost 'covid-19' prediction but works when only keep the first sentences. prediction by sentence?
# test = [['id1', """COVID-19 advice from government sometimes lacks detail for their situation. They are a mixture of lab and office space. They suggested specific examples of implementing the advice for laboratories would be helpful.
# The UK is still considered a good place to invest for companies who are already located there but Sweden and Denmark are now as attractive
# for new investors due to uncertainty in the UK during the EU exit transition period."""]]
# # test = [['id1', """COVID-19 advice from government sometimes lacks detail for their situation."""]]
# df_a = pd.DataFrame(test, columns=['id', 'policy feedback'])
# df_a = preprocess(df_a, action='predict')
# os.chdir('/Users/linglingli/DIT/data-flow/the_models/models_20201029')
# predictions_a = predict_tags(df_a)
# print(predictions_a['tags_prediction'])