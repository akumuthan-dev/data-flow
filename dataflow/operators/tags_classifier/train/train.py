from airflow.hooks.postgres_hook import PostgresHook
from dataflow.utils import logger, S3Data
import numpy
import operator

# fix random seed for reproducibility
numpy.random.seed(7)
from tempfile import TemporaryDirectory

from airflow.hooks.S3_hook import S3Hook


# from dataflow.operators.tags_classifier_train.setting import tags_covid, tags_general
# from dataflow.operators.tags_classifier_train.utils import *


from dataflow.operators.tags_classifier.setting import *
from dataflow.operators.tags_classifier.utils import *

print('check it out', tags_general)

from dataflow import config
import boto3
import shutil
import datetime
import os

print(os.getcwd(), __package__, __name__, __file__)

from sklearn.model_selection import train_test_split
import numpy as np
from itertools import compress
import json


#
# def fetch_interaction_labelled_data(
#     target_db: str,  query: str
# ):
#     logger.info("starting fetching data")
#     print(sys.version)
#     print(tf.__version__)
#
#     try:
#         print('working directory', os.getcwd())
#         # create connection with named cursor to fetch data in batches
#         connection = PostgresHook(postgres_conn_id=target_db).get_conn()
#         cursor = connection.cursor(name='fetch_interaction')
#         cursor.execute(query)
#
#         rows = cursor.fetchall()
#         df = pd.DataFrame(rows)
#         print(df.shape)
#         df.columns = [column[0] for column in cursor.description]
#         df = preprocess(df, action='train', tags=all_tags)
#         df.to_csv('to_train.csv', index=False)
#         logger.info(df)
#
#
#     finally:
#         if connection:
#             cursor.close()
#             connection.close()
#
#     return df


def fetch_interaction_labelled_data(training_file):
    bucket = config.S3_IMPORT_DATA_BUCKET
    logger.info(f"Bucket: {bucket}")

    client = S3Hook("DEFAULT_S3")

    s3_key_object = client.get_key(
        'models/data_hub_policy_feedback_tags_classifier/' + training_file,
        bucket_name=bucket,
    )

    # note: 'wb' rather than 'w'
    with open(training_file, 'wb') as data:
        s3_key_object.download_fileobj(data)

    logger.info(f"working dir: {os.getcwd()}")
    logger.info(f"list contents: {os.listdir()}")

    df = pd.read_csv(training_file)
    df = preprocess(df, action='train', tags=all_tags)

    return df


def build_tokens(df, model_path):
    from tensorflow.keras.preprocessing.text import Tokenizer

    # model_path = "models_" + today + "/models_general/"
    # print(df.columns)
    tokenizer = Tokenizer(
        num_words=MAX_NB_WORDS,
        filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n',
        lower=True,
    )
    # tokenizer = Tokenizer(num_words=MAX_NB_WORDS, filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~', lower=True)
    tokenizer.fit_on_texts(df['sentence'].values)
    word_index = tokenizer.word_index
    print('Found %s unique tokens.' % len(word_index))

    tokenizer_json = tokenizer.to_json()
    os.makedirs(model_path)
    with open(model_path + 'cnn_tokenizer.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(tokenizer_json, ensure_ascii=False))

    return tokenizer, tokenizer_json


# with open('models_covid/cnn_tokenizer_covid.pickle', 'wb') as handle:
#     pickle.dump(tokenizer, handle, protocol=pickle.HIGHEST_PROTOCOL)


# def transform_X(X_text, tokenizer):
#     X = tokenizer.texts_to_sequences(X_text)
#     X = sequence.pad_sequences(X, maxlen=MAX_SEQUENCE_LENGTH, padding='post', truncating='post')
#     return X


def build_train_set(df, tokenizer, tags):
    from tensorflow.keras.preprocessing import sequence

    print('MAX_SEQUENCE_LENGTH', MAX_SEQUENCE_LENGTH)
    X = tokenizer.texts_to_sequences(df['sentence'].values)
    X = sequence.pad_sequences(
        X, maxlen=MAX_SEQUENCE_LENGTH, padding='post', truncating='post'
    )
    print('Shape of data tensor:', X.shape)

    Y = df[tags].values
    # Y = df['label'].values.reshape(-1,1)
    print('Shape of label tensor:', Y.shape)

    sent_train, sent_test, X_train, X_test, Y_train, Y_test = train_test_split(
        df['sentence'].values, X, Y, test_size=0.10, random_state=42, shuffle=True
    )
    print(X_train.shape, Y_train.shape)
    print(X_test.shape, Y_test.shape)

    sent_train, sent_val, X_train, X_val, Y_train, Y_val = train_test_split(
        sent_train, X_train, Y_train, test_size=0.10, random_state=42, shuffle=True
    )

    # import pickle
    # with open('check_0.pickle', 'wb') as handle:
    #     pickle.dump(X_train, handle, protocol=pickle.HIGHEST_PROTOCOL)

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
    # model.add(layers.Embedding(input_dim=vocab_size,
    #                            output_dim=embedding_dim,
    #                            input_length=maxlen))
    # model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1],trainable=True,mask_zero=True, weights=[embedding_matrix]))
    # model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1],trainable=True, weights=[embedding_matrix]))
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

        try:
            train_data_date = training_data_file.split('.')[0].split('_')[-1]
        except:
            train_data_date = datetime.date.today()
            train_data_date = train_data_date.strftime("%Y%m%d")

        logger.info("step 1: fetch data")
        df = fetch_interaction_labelled_data(training_data_file)
        print('check cwd', os.getcwd())
        # df = pd.read_csv('/home/vcap/app/dataflow/dags/training_data_20201001.csv')
        # df = preprocess(df, action='train', tags=all_tags)

        logger.info("step 2: train model")
        build_models_pipeline(df, train_data_date)

        logger.info("step 3: save model")
        save_model(train_data_date)

        logger.info("step 4: write model performance to S3")
        write_model_performance(table_name, train_data_date, **context)

    pass


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
            tf.keras.metrics.Recall(name='recall')
            # , tf.keras.metrics.binary_crossentropy
            ,
            tf.keras.metrics.BinaryCrossentropy(name='entropy'),
            tf.keras.metrics.TruePositives(name='tp'),
            tf.keras.metrics.TrueNegatives(name='tn'),
            tf.keras.metrics.FalsePositives(name='fp'),
            tf.keras.metrics.FalseNegatives(name='fn'),
        ],
    )

    epochs = 20
    batch_size = 64

    history = model.fit(
        X_train,
        Y_train,
        epochs=epochs,
        # class_weight='balanced', #
        class_weight={0: 1, 1: class_weight},
        batch_size=batch_size,
        # validation_split=0.1,
        validation_data=(X_val, Y_val),
        # callbacks=[EarlyStopping(monitor='val_auc', patience=3, min_delta=0.0001, mode='max', restore_best_weights=True)],
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

    for j in tags:
        # if j!='Covid-19 Employment':
        if 1 == 1:
            print('-------------' * 5)
            print('model for ' + j)
            i = tags.index(j)

            Y_train_tag = Y_train[:, i]
            print('check y', Y_train_tag.sum(), Y_train_tag.shape)
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
            # class_weight = (sum(Y_test[:,tag_i]==0)+ sum(Y_train[:,tag_i]==0))/ (sum(Y_test[:,tag_i]==1)+ sum(Y_train[:,tag_i]==1))

            print('class_weight', class_weight)

            # print('X_train', X_train.shape, 'ddddd')
            m = cnn()
            m = train_model(m, X_train, Y_train_tag, X_val, Y_val_tag, class_weight)
            # m.save("./models_covid/"+'_'.join(j.split(' ')))
            m.save(model_path + '_'.join(j.split(' ')))

            data_to_evaluate = X_test
            tag_to_evaluate = Y_test_tag
            sent_to_evaluate = sent_test
            # data_to_evaluate = X_val
            # tag_to_evaluate = Y_val_tag
            # sent_to_evalue = sent_val

            metrics = m.evaluate(
                data_to_evaluate, tag_to_evaluate, batch_size=tag_to_evaluate.shape[0]
            )

            test_predictions_prob_tag = m.predict(data_to_evaluate)
            test_predictions_class_tag = (test_predictions_prob_tag > probability_threshold) + 0
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

    print('check columns', metric_df.columns)
    metric_df.to_csv(model_path + '/models_metrics_cnn.csv', index=False)

    actual = []
    predict = []
    sentence = []

    for i in np.arange(0, sent_to_evaluate.shape[0]):
        sentence.append(sent_to_evaluate[i])
        actual.append(list(compress(tags, Y_test[i])))
        predict.append(list(compress(tags, Y_test_predict_prob[i] > probability_threshold)))

    return metric_df


def check_result(tag, tag_i, X_test, Y_test, sent_test, m):
    print(tag)

    test_predictions_prob_tag1 = m.predict(X_test)
    test_predictions_class_tag1 = (test_predictions_prob_tag1 > probability_threshold) + 0

    # threshold = probability_threshold
    # fp_ind = np.concatenate((test_predictions_prob_tag1>=threshold)&(Y_test[:,tag_i]==0))
    fp_sen = sent_test[
        (np.concatenate(test_predictions_prob_tag1) > probability_threshold)
        & (Y_test[:, tag_i] == 0)
    ]
    fp_p = test_predictions_prob_tag1[
        np.concatenate((test_predictions_prob_tag1 >= probability_threshold))
        & (Y_test[:, tag_i] == 0)
    ]

    # tp_ind = np.concatenate((test_predictions_prob_tag1>=threshold)&(Y_test[:,tag_i]==1))
    tp_sen = sent_test[
        (np.concatenate(test_predictions_prob_tag1) > probability_threshold)
        & (Y_test[:, tag_i] == 1)
    ]
    tp_p = test_predictions_prob_tag1[
        np.concatenate((test_predictions_prob_tag1 >= probability_threshold))
        & (Y_test[:, tag_i] == 1)
    ]

    # fn_ind = np.concatenate((test_predictions_prob_tag1<threshold)&(Y_test[:,tag_i]==1))
    fn_sen = sent_test[
        (np.concatenate(test_predictions_prob_tag1) <= probability_threshold)
        & (Y_test[:, tag_i] == 1)
    ]
    fn_p = test_predictions_prob_tag1[
        np.concatenate((test_predictions_prob_tag1 < probability_threshold))
        & (Y_test[:, tag_i] == 1)
    ]

    # tn_ind = np.concatenate((test_predictions_prob_tag1<threshold)&(Y_test[:,tag_i]==0))
    tn_sen = sent_test[
        (np.concatenate(test_predictions_prob_tag1) <= threshold)
        & (Y_test[:, tag_i] == 0)
    ]
    tn_p = test_predictions_prob_tag1[
        np.concatenate((test_predictions_prob_tag1 < threshold))
        & (Y_test[:, tag_i] == 0)
    ]

    len(fp_sen), len(tp_sen), len(fn_sen), len(tn_sen), len(fp_p), len(tp_p), len(
        fn_p
    ), len(tn_p)

    return fp_sen, tp_sen, fn_sen, tn_sen, fp_p, tp_p, fn_p, tn_p


# fp_sen, tp_sen, fn_sen, tn_sen, fp_p, tp_p, fn_p, tn_p = check_result(tag_i, X_test,Y_test, sent_test)


def build_models_pipeline(df, today):

    # today = datetime.date.today()
    #     # today = today.strftime("%Y%m%d")

    # print('tags_general', tags_general)
    # print('tags_covid', tags_covid)
    # if run_mode=='prod':
    #     df = context['task_instance'].xcom_pull(task_ids='fetch-interaction-data')
    # elif run_mode=='dev':
    #     df = pd.read_csv(context['data'])
    #     df['id'] = 'placeholder'

    # if len(tags_general)>0:
    if len(tags_general) > 0 and tags_general != ['Covid-19']:  ##todo remove the 2nd

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
        metric_df_general = buid_models_for_tags(
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
        metric_df_covid = buid_models_for_tags(
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
    ## todo save to prod bucket if use this model in prod
    bucket = config.S3_IMPORT_DATA_BUCKET
    # bucket = 'paas-s3-broker-prod-lon-f516a2f5-a71b-43e0-88ed-da39437cde6a' ##instance name: data-flow-s3

    shutil.make_archive('models_' + train_data_date, 'zip', 'models_' + train_data_date)

    s3 = boto3.client('s3', region_name='eu-west-2')
    s3.upload_file(
        'models_' + train_data_date + '.zip',
        bucket,
        'models/data_hub_policy_feedback_tags_classifier/' + 'models_' + train_data_date + '.zip',
    )

    # print('bucket', bucket)
    #
    # # cf service-key data-flow-s3-a dev-key-name-for-LL
    # bucket = "paas-s3-broker-prod-lon-8c167200-ac5c-4a68-80c0-dd079c35b1be"  ##instance name: data-flow-s3-a
    # bucket = 'paas-s3-broker-prod-lon-49ad84ab-6294-44b7-8883-e1b5488208fa' ##prod
    # print('bucket', bucket)
    # aws_access_key_id = 'AKIAV3ON3AJYBPYZLL6V'  ##data-flow-s3-a
    # aws_secret_access_key = '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'  ##data-flow-s3-a
    # s3 = boto3.client(
    #     's3',
    #     region_name='eu-west-2',
    #     aws_access_key_id=aws_access_key_id,
    #     aws_secret_access_key=aws_secret_access_key,
    # )
    # # s3.upload_file('/Users/linglingli/DIT/data-flow/dataflow/operators/tags_classifier_train/models_20200907.zip',bucket,
    # #                'models/data_hub_policy_feedback_tags_classifier/models_20200907.zip')
    # s3.upload_file(
    #     'models_' + train_data_date + '.zip',
    #     bucket,
    #     'models/data_hub_policy_feedback_tags_classifier/'
    #     + 'models_'
    #     + train_data_date
    #     + '.zip',
    # )

    return None


def write_model_performance(table_name, today, **context):
    #     today = datetime.date.today()
    #     today = today.strftime("%Y%m%d")

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

    return None


#
#
# run_mode='dev'
# #
# # a = pd.read_csv('interaction_all_data_0811.csv', encoding='utf-8-sig')
# # a['id'] = 'placeholder'
# # a = a.dropna()
# # a['biu_issue_type'] = a['biu_issue_type'].apply(lambda x: x.replace(';', ',').replace('\u200b', '').replace('â€‹', '').replace('Â', ''))
# # b = pd.read_csv('training_data_0901.csv', encoding='utf-8-sig')
# # b['id'] = 'placeholder'
# # b = b[['policy_feedback_notes','biu_issue_type', 'id']]
# # b = b.dropna()
# # b['biu_issue_type'] = b['biu_issue_type'].apply(lambda x: x.replace(';', ',').replace('\u200b', '').replace('â€‹', '').replace('Â', ''))
# # c = pd.read_csv('data_for_lingling_select_interaction_all_data_0819.csv', encoding='utf-8-sig')
# # c['id'] = 'placeholder'
# # c = c[['policy_feedback_notes','biu_issue_type', 'id']]
# # c = c.dropna()
# # c['biu_issue_type'] = c['biu_issue_type'].apply(lambda x: x.replace(';', ',').replace('\u200b', '').replace('â€‹', '').replace('Â', ''))
# # d = pd.read_csv('training_data_0904.csv', encoding='utf-8-sig')
# # d['id'] = 'placeholder'
# # d = d[['policy_feedback_notes','biu_issue_type', 'id']]
# # d = d.dropna()
# # d['biu_issue_type'] = d['biu_issue_type'].apply(lambda x: x.replace(';', ',').replace('\u200b', '').replace('â€‹', '').replace('Â', ''))
# # df =  pd.concat([a,b,c,d])
# # df = df.dropna()
# # df2 = df.drop_duplicates()
# # df3 = df[['policy_feedback_notes']].drop_duplicates()
# # print(df.shape, df2.shape,df3.shape,a.shape,b.shape,c.shape,d.shape)
# # df = df2.copy()
# #
# #
# # x = pd.concat([a, d])
# # x = x.drop_duplicates()
# # x.columns = ['policy feedback', 'biu_issue_type', 'id']
# # x=exclude_notes(x)
# # xg = x.groupby('policy feedback').count()
# # xg=xg[xg['biu_issue_type']>1]
#
#
# # df = pd.read_csv('interaction_all_data_0811.csv')
# # df = df.drop_duplicates()
# # df = pd.read_csv('data_for_lingling_select_interaction_all_data_0819.csv')
# # df = pd.read_csv('training_data_0901.csv')
# # df = pd.read_csv('training_data_0904.csv')
# # df['id'] = 'placeholder'
#
#
# # df0 = pd.read_csv("covid19.csv")
# # df0 = df0[['Policy Feedback Notes','Biu Issue Types', 'Interaction ID']]
# # df0.columns = ['policy_feedback_notes', 'biu_issue_type', 'id']
# # df0 = df0.dropna()
#
# # df = pd.read_csv("covid19.csv")
# # df = df[['Interaction ID', 'Policy Feedback Notes', 'Biu Issue Types']]
# # df.columns = ['id', 'policy_feedback_notes', 'biu_issue_type']
#
#
# df = pd.read_csv('training_data_0904.csv')
# df = preprocess(df, action='train', tags=all_tags)
# # print('check 3', df.shape)
# df.to_csv('to_train.csv', index=False)
# build_models_pipeline(data='to_train.csv')
# #
# # for j in np.arange(0,5):
# #   print(j, '-------'*5)
# #   # print(tp_p[j])
# #   # print(np.array(tp_sen)[j])
# #   print(fp_p[j])
# #   print(np.array(fp_sen)[j])
# #   # print(fn_p[j])
# #   # print(fn_sen[j])
#
# # df[df['sentence'].str.startswith('Covid-19 Track & Trace, SSP rebates, EU Exit,')]
#
# # build_models_pipeline(data='to_train_0901_check.csv')


## to upload the file to s3
# s3 = boto3.client('s3', region_name='eu-west-2')
# bucket = 'paas-s3-broker-prod-lon-f516a2f5-a71b-43e0-88ed-da39437cde6a' ##instance name: data-flow-s3
# # s3.upload_file('/Users/linglingli/DIT/data-flow/dataflow/dags/training_data_20201029.csv',
# #                    bucket,
# #                  'models/data_hub_policy_feedback_tags_classifier/training_data_20201029.csv')
# s3.upload_file('/Users/linglingli/DIT/data-flow/training_data_20201029.csv',
#                    bucket,
#                  'models/data_hub_policy_feedback_tags_classifier/training_data_20201029.csv')

# cf service-key data-flow-s3-a dev-key-name-for-LL
# bucket = "paas-s3-broker-prod-lon-8c167200-ac5c-4a68-80c0-dd079c35b1be" ##instance name: data-flow-s3-a
# aws_access_key_id = 'AKIAV3ON3AJYBPYZLL6V' ##data-flow-s3-a
# aws_secret_access_key  = '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'  ##data-flow-s3-a
# s3 = boto3.client('s3', region_name='eu-west-2', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
# s3.upload_file('/Users/linglingli/DIT/data-flow/dataflow/operators/tags_classifier_train/models_20200907.zip',bucket,
#                'models/data_hub_policy_feedback_tags_classifier/models_20200907.zip')


# # run this if want to run locally
# # df = pd.read_csv('/Users/linglingli/DIT/data-flow/dataflow/dags/training_data_20201001.csv')
# df = pd.read_csv('/Users/linglingli/DIT/data-flow/training_data_20201029.csv')
# df = preprocess(df, action='train', tags=all_tags)

# # to check
# # df[df['sentence'].str.contains('migration')].head(1)[['sentence', 'Migration And Immigration']]
# # df[(df['sentence'].str.contains('border'))&(df['Border Arrangements']==0)].head(10).reset_index().iloc[3]['sentence']
# # # print('check 3', df.shape)
# # df.to_csv('to_train.csv', index=False)
# train_data_date = training_data_file.split('.')[0].split('_')[-1]
# build_models_pipeline(df, train_data_date)