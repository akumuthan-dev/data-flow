from dataflow.utils import logger, S3Data
import numpy
from tempfile import TemporaryDirectory
from airflow.hooks.S3_hook import S3Hook

from dataflow.operators.tags_classifier.utils import (
    preprocess,
    report_metric_per_model,
    report_metrics_for_all,
)
from dataflow.operators.tags_classifier.setting import (
    MAX_NB_WORDS,
    MAX_SEQUENCE_LENGTH,
    EMBEDDING_DIM,
    probability_threshold,
)

from dataflow import config
import boto3
import shutil
import datetime
import os
import pandas as pd


from sklearn.model_selection import train_test_split
import numpy as np
from itertools import compress
import json

# fix random seed for reproducibility
numpy.random.seed(7)


def fetch_interaction_labelled_data():
    bucket = config.S3_IMPORT_DATA_BUCKET
    logger.info(f"Bucket: {bucket}")

    client = S3Hook("DEFAULT_S3")

    all_training_files = [
        i
        for i in client.list_keys(
            bucket, prefix='models/data_hub_policy_feedback_tags_classifier/'
        )
    ]
    all_training_files = [i.split('/')[-1] for i in all_training_files]
    all_training_files = [
        i
        for i in all_training_files
        if i.lower().startswith('training_data') and i.lower().endswith('csv')
    ]

    all_training_files.sort()
    training_file_name = all_training_files[-1]

    logger.info(f'All training files in the bucket: {all_training_files}')
    logger.info(f'The file used for training: {training_file_name}')

    s3_key_object = client.get_key(
        'models/data_hub_policy_feedback_tags_classifier/' + training_file_name,
        bucket_name=bucket,
    )

    # note: 'wb' rather than 'w'
    with open(training_file_name, 'wb') as data:
        s3_key_object.download_fileobj(data)

    logger.info(f"working dir: {os.getcwd()}")
    logger.info(f"list contents: {os.listdir()}")

    df = pd.read_csv(training_file_name)
    df, tags_to_train = preprocess(df, action='train')

    return df, tags_to_train, training_file_name


def build_tokens(df, model_path):
    from tensorflow.keras.preprocessing.text import Tokenizer

    tokenizer = Tokenizer(
        num_words=MAX_NB_WORDS,
        filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
        lower=True,
    )

    tokenizer.fit_on_texts(df['sentence'].values)
    word_index = tokenizer.word_index
    logger.info(f'Found {len(word_index)} unique tokens')

    tokenizer_json = tokenizer.to_json()
    os.makedirs(model_path)
    with open(model_path + 'cnn_tokenizer.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(tokenizer_json, ensure_ascii=False))

    return tokenizer, tokenizer_json


def build_train_set(df, tokenizer, tags):
    from tensorflow.keras.preprocessing import sequence

    logger.info(f"MAX_SEQUENCE_LENGTH: {MAX_SEQUENCE_LENGTH}")
    X = tokenizer.texts_to_sequences(df['sentence'].values)
    X = sequence.pad_sequences(
        X, maxlen=MAX_SEQUENCE_LENGTH, padding='post', truncating='post'
    )
    logger.info(f"Shape of data tensor: {X.shape}")

    Y = df[tags].values
    logger.info(f"Shape of label tensor: {Y.shape}")

    sent_train, sent_test, X_train, X_test, Y_train, Y_test = train_test_split(
        df['sentence'].values, X, Y, test_size=0.10, random_state=42, shuffle=True
    )

    logger.info(f"Shape of X_train: {X_train.shape}")
    logger.info(f"Shape of Y_train: {Y_train.shape}")
    logger.info(f"Shape of X_test: {X_test.shape}")
    logger.info(f"Shape of Y_test: {Y_test.shape}")

    sent_train, sent_val, X_train, X_val, Y_train, Y_val = train_test_split(
        sent_train, X_train, Y_train, test_size=0.10, random_state=42, shuffle=True
    )

    return (
        sent_train,
        sent_val,
        sent_test,
        X_train,
        X_val,
        X_test,
        Y_train,
        Y_val,
        Y_test,
    )


def cnn():
    from tensorflow.keras.models import Sequential
    from tensorflow.keras import layers
    from tensorflow.keras.layers import Embedding

    model = Sequential()
    model.add(
        Embedding(
            MAX_NB_WORDS,
            EMBEDDING_DIM,
            input_length=MAX_SEQUENCE_LENGTH,
            trainable=True,
        )
    )
    model.add(layers.Conv1D(128, 5, activation='relu'))
    model.add(layers.GlobalMaxPool1D())
    # model.add(layers.Flatten())
    model.add(layers.Dense(8, activation='relu'))
    model.add(layers.Dropout(0.2))
    model.add(layers.Dense(1, activation='sigmoid'))
    # model.compile(optimizer='adam',
    #               loss='binary_crossentropy',
    #               metrics=['accuracy'])

    return model


def model_training_with_labelled_data(table_name, **context):

    with TemporaryDirectory() as tempdir:
        # if 1 == 1:
        #     tempdir = '.'
        os.chdir(tempdir)
        os.mkdir('the_models')
        os.chdir(tempdir + '/the_models')
        logger.info(f"working dir: {os.getcwd()}")

        logger.info("step 1: fetch data")
        df, tags_to_train, training_file_name = fetch_interaction_labelled_data()
        # to test without trigger the pipeline:
        # df = pd.read_csv('/home/vcap/app/dataflow/dags/training_data_20201001.csv')
        # df = preprocess(df, action='train', tags=all_tags)

        try:
            train_data_date = training_file_name.split('.')[0].split('_')[-1]
        except BaseException:
            train_data_date = datetime.date.today()
            train_data_date = train_data_date.strftime("%Y%m%d")

        logger.info("step 2: train model")
        build_models_pipeline(df, train_data_date, tags_to_train)

        logger.info("step 3: save model to S3")
        save_model(train_data_date)

        logger.info("step 4: write model performance to S3")
        write_model_performance(table_name, train_data_date, **context)


def train_model(model, X_train, Y_train, X_val, Y_val, class_weight):
    from tensorflow.keras.callbacks import EarlyStopping
    import tensorflow as tf

    tf.random.set_seed(2)

    model.compile(
        loss='binary_crossentropy',
        optimizer='adam',
        metrics=[
            #  tf.keras.metrics.categorical_accuracy,
            tf.keras.metrics.AUC(name='auc'),
            tf.keras.metrics.Precision(name='precision'),
            tf.keras.metrics.Recall(name='recall'),
            #  tf.keras.metrics.binary_crossentropy
            tf.keras.metrics.BinaryCrossentropy(name='entropy'),
            tf.keras.metrics.TruePositives(name='tp'),
            tf.keras.metrics.TrueNegatives(name='tn'),
            tf.keras.metrics.FalsePositives(name='fp'),
            tf.keras.metrics.FalseNegatives(name='fn'),
        ],
    )

    epochs = 20
    batch_size = 64

    model.fit(
        X_train,
        Y_train,
        epochs=epochs,
        # class_weight='balanced', #
        class_weight={0: 1, 1: class_weight},
        batch_size=batch_size,
        # validation_split=0.1,
        validation_data=(X_val, Y_val),
        callbacks=[
            EarlyStopping(
                monitor='val_entropy',
                patience=3,
                min_delta=0.0001,
                mode='min',
                restore_best_weights=True,
            )
        ],
        verbose=0
        # callbacks=[EarlyStopping(monitor='val_auc', patience=3, min_delta=0.0001, mode='max', restore_best_weights=True)],
        # callbacks=[EarlyStopping(monitor='val_loss', patience=3, min_delta=0.0001, mode='min', restore_best_weights=True)]
        # callbacks=[tensorboard_callback]
    )

    return model


def buid_models_for_tags(
    tags, sent_test, X_train, X_val, Y_train, Y_val, X_test, Y_test, model_path
):
    tag_precisions = dict()
    tag_recalls = dict()
    tag_f1 = dict()
    tag_accuracy = dict()
    tag_auc = dict()
    tag_size = dict()

    Y_test_predict = np.zeros(Y_test.shape)
    Y_test_predict_prob = np.zeros(Y_test.shape)

    logger.info(f"build models for tags:{tags}")

    for j in tags:
        logger.info('-------------' * 5)
        logger.info(f'model for {j}')
        i = tags.index(j)

        Y_train_tag = Y_train[:, i]
        logger.info(
            f'check Y_train_tag:  positive case:{Y_train_tag.sum()}; shape:{Y_train_tag.shape}'
        )
        Y_test_tag = Y_test[:, i]
        Y_val_tag = Y_val[:, i]
        class_size = (
            sum(Y_test[:, i] == 1) + sum(Y_train[:, i] == 1),
            (sum(Y_test[:, i] == 1) + sum(Y_train[:, i] == 1))
            / (Y_test.shape[0] + Y_train.shape[0]),
        )
        class_weight = (sum(Y_test[:, i] == 0) + sum(Y_train[:, i] == 0)) / (
            sum(Y_test[:, i] == 1) + sum(Y_train[:, i] == 1)
        )
        logger.info(f'check class_weight:  {class_weight}')

        m = cnn()
        m = train_model(m, X_train, Y_train_tag, X_val, Y_val_tag, class_weight)
        m.save(model_path + '_'.join(j.split(' ')))

        data_to_evaluate = X_test
        tag_to_evaluate = Y_test_tag
        sent_to_evaluate = sent_test

        test_predictions_prob_tag = m.predict(data_to_evaluate)
        test_predictions_class_tag = (
            test_predictions_prob_tag > probability_threshold
        ) + 0
        Y_test_predict[:, i] = np.concatenate((test_predictions_class_tag))
        Y_test_predict_prob[:, i] = np.concatenate((test_predictions_prob_tag))

        precisions, recalls, f1, accuracy, auc = report_metric_per_model(
            tag_to_evaluate, test_predictions_class_tag, average_type='binary'
        )

        tag_precisions[tags[i]] = precisions
        tag_recalls[tags[i]] = recalls
        tag_f1[tags[i]] = f1
        tag_accuracy[tags[i]] = accuracy
        tag_auc[tags[i]] = auc
        tag_size[tags[i]] = class_size

        # global fp_sen, tp_sen, fn_sen, tn_sen, fp_p, tp_p, fn_p, tn_p
        # fp_sen, tp_sen, fn_sen, tn_sen, fp_p, tp_p, fn_p, tn_p = check_result(tags[i], i, X_test,Y_test, sent_test,m)

    metric_df = report_metrics_for_all(
        tag_size, tag_precisions, tag_recalls, tag_f1, tag_accuracy, tag_auc
    )

    logger.info(f'check metric columns:  {metric_df.columns}')
    metric_df.to_csv(model_path + '/models_metrics_cnn.csv', index=False)

    actual = []
    predict = []
    sentence = []

    for i in np.arange(0, sent_to_evaluate.shape[0]):
        sentence.append(sent_to_evaluate[i])
        actual.append(list(compress(tags, Y_test[i])))
        predict.append(
            list(compress(tags, Y_test_predict_prob[i] > probability_threshold))
        )

    return metric_df


def check_result(tag, tag_i, X_test, Y_test, sent_test, m):
    # fp_sen, tp_sen, fn_sen, tn_sen, fp_p, tp_p, fn_p, tn_p = check_result(tag_i, X_test,Y_test, sent_test)
    logger.info(f'check result for tag {tag}')

    test_predictions_prob_tag1 = m.predict(X_test)
    # test_predictions_class_tag1 = (
    #     test_predictions_prob_tag1 > probability_threshold
    # ) + 0

    # fp_ind = np.concatenate((test_predictions_prob_tag1>=probability_threshold)&(Y_test[:,tag_i]==0))
    fp_sen = sent_test[
        (np.concatenate(test_predictions_prob_tag1) > probability_threshold)
        & (Y_test[:, tag_i] == 0)
    ]
    fp_p = test_predictions_prob_tag1[
        np.concatenate((test_predictions_prob_tag1 >= probability_threshold))
        & (Y_test[:, tag_i] == 0)
    ]

    # tp_ind = np.concatenate((test_predictions_prob_tag1>=probability_threshold)&(Y_test[:,tag_i]==1))
    tp_sen = sent_test[
        (np.concatenate(test_predictions_prob_tag1) > probability_threshold)
        & (Y_test[:, tag_i] == 1)
    ]
    tp_p = test_predictions_prob_tag1[
        np.concatenate((test_predictions_prob_tag1 >= probability_threshold))
        & (Y_test[:, tag_i] == 1)
    ]

    # fn_ind = np.concatenate((test_predictions_prob_tag1<probability_threshold)&(Y_test[:,tag_i]==1))
    fn_sen = sent_test[
        (np.concatenate(test_predictions_prob_tag1) <= probability_threshold)
        & (Y_test[:, tag_i] == 1)
    ]
    fn_p = test_predictions_prob_tag1[
        np.concatenate((test_predictions_prob_tag1 < probability_threshold))
        & (Y_test[:, tag_i] == 1)
    ]

    # tn_ind = np.concatenate((test_predictions_prob_tag1<probability_threshold)&(Y_test[:,tag_i]==0))
    tn_sen = sent_test[
        (np.concatenate(test_predictions_prob_tag1) <= probability_threshold)
        & (Y_test[:, tag_i] == 0)
    ]
    tn_p = test_predictions_prob_tag1[
        np.concatenate((test_predictions_prob_tag1 < probability_threshold))
        & (Y_test[:, tag_i] == 0)
    ]

    len(fp_sen), len(tp_sen), len(fn_sen), len(tn_sen), len(fp_p), len(tp_p), len(
        fn_p
    ), len(tn_p)

    return fp_sen, tp_sen, fn_sen, tn_sen, fp_p, tp_p, fn_p, tn_p


def build_models_pipeline(df, today, tags_to_train):
    # if useing multiple operators:
    # df = context['task_instance'].xcom_pull(task_ids='fetch-interaction-data')

    tags_covid = [
        i
        for i in tags_to_train
        if i.lower().startswith('covid') and i.lower() != 'covid-19'
    ]

    tags_general = [i for i in tags_to_train if i not in tags_covid]

    if len(tags_general) > 0:

        model_path = "models_" + today + "/models_general/"
        tokenizer_general, tokenizer_json_general = build_tokens(df, model_path)
        (
            sent_train,
            sent_val,
            sent_test,
            X_train,
            X_val,
            X_test,
            Y_train,
            Y_val,
            Y_test,
        ) = build_train_set(df, tokenizer_general, tags_general)

        # metric_df_general = \
        buid_models_for_tags(
            tags_general,
            sent_test,
            X_train,
            X_val,
            Y_train,
            Y_val,
            X_test,
            Y_test,
            model_path=model_path,
        )

    if len(tags_covid) > 0:
        model_path = "models_" + today + "/models_covid/"
        df_covid = df[df['Covid-19'] > 0][['sentence'] + tags_covid]
        tokenizer_covid, tokenizer_json_covid = build_tokens(df_covid, model_path)
        (
            sent_train,
            sent_val,
            sent_test,
            X_train,
            X_val,
            X_test,
            Y_train,
            Y_val,
            Y_test,
        ) = build_train_set(df_covid, tokenizer_covid, tags_covid)

        # metric_df_covid = \
        buid_models_for_tags(
            tags_covid,
            sent_test,
            X_train,
            X_val,
            Y_train,
            Y_val,
            X_test,
            Y_test,
            model_path=model_path,
        )

    # return metric_df_general, metric_df_covid ## or: merge these two tables, and pass it to context for next step
    return None


def save_model(train_data_date):
    bucket = config.S3_IMPORT_DATA_BUCKET

    shutil.make_archive('models_' + train_data_date, 'zip', 'models_' + train_data_date)

    model_version = 'models_' + train_data_date + '.zip'
    logger.info(f"this model version is: {model_version}")

    s3 = boto3.client('s3', region_name='eu-west-2')
    s3.upload_file(
        model_version,
        bucket,
        'models/data_hub_policy_feedback_tags_classifier/' + model_version,
    )

    return None


def write_model_performance(table_name, today, **context):

    metrics1 = pd.read_csv('models_' + today + '/models_covid/models_metrics_cnn.csv')
    metrics2 = pd.read_csv('models_' + today + '/models_general/models_metrics_cnn.csv')

    metrics = pd.concat([metrics1, metrics2])
    metrics = metrics.reset_index()
    # metrics = context['task_instance'].xcom_pull(task_ids='build-model')

    metrics['model_version'] = 'models_' + today
    cols = metrics.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    metrics = metrics[cols]

    metrics_json = metrics.to_json(orient="records")
    metrics_json = json.loads(metrics_json)

    s3 = S3Data(table_name, context["ts_nodash"])
    s3.write_key('model_performance.json', metrics_json)

    logger.info(f"performance for this model version is: {metrics}")

    return None


# # to upload  file/model to s3
# cf service-key data-flow-s3-a dev-key-name-for-LL
# aws_access_key_id, aws_secret_access_key, bucket
# s3 = boto3.client('s3', region_name='eu-west-2')
# s3 = boto3.client('s3', region_name='eu-west-2', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
# s3.upload_file('/Users/linglingli/DIT/data-flow/training_data_20201127.csv',
#                    bucket,
#                  'models/data_hub_policy_feedback_tags_classifier/training_data_20201127.csv')
# s3.upload_file('/Users/linglingli/DIT/data-flow/dataflow/operators/tags_classifier_train/models_20200907.zip',bucket,
#                'models/data_hub_policy_feedback_tags_classifier/models_20200907.zip')


# # run this if want to run locally
# df = pd.read_csv('/Users/linglingli/DIT/data-flow/training_data_20201029.csv')
# df = preprocess(df, action='train', tags=all_tags)
