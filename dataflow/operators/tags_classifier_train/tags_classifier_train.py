from airflow.hooks.postgres_hook import PostgresHook
from dataflow.utils import logger, S3Data
import numpy
import operator
# fix random seed for reproducibility
numpy.random.seed(7)

from dataflow.operators.tags_classifier_train.setting import tags_covid, tags_general

print('check it out', tags_general)

from dataflow import config
import boto3
import shutil
import datetime
import os
print(os.getcwd(), __package__, __name__, __file__)
from keras_preprocessing.text import tokenizer_from_json
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Embedding, Conv1D, GlobalMaxPooling1D, Flatten
from tensorflow.keras.preprocessing import sequence
import tensorflow as tf
from tensorflow.keras.preprocessing.text import Tokenizer
from sklearn.model_selection import train_test_split
from tensorflow.keras.callbacks import EarlyStopping, TensorBoard
import numpy as np
from tensorflow.keras import layers
from itertools import compress
import json
from dataflow.operators.tags_classifier_train.utils import *
import sys

print('a', a)

def fetch_interaction_labelled_data(
    target_db: str,  query: str
):
    logger.info("starting fetching data")
    print(sys.version)
    print(tf.__version__)

    try:
        print('working directory', os.getcwd())
        # create connection with named cursor to fetch data in batches
        connection = PostgresHook(postgres_conn_id=target_db).get_conn()
        cursor = connection.cursor(name='fetch_interaction')
        cursor.execute(query)

        rows = cursor.fetchall()
        df = pd.DataFrame(rows)
        print(df.shape)
        df.columns = [column[0] for column in cursor.description]
        df = preprocess(df, action='train', tags=tags)
        df.to_csv('to_train.csv', index=False)
        logger.info(df)


    finally:
        if connection:
            cursor.close()
            connection.close()

    return df


def build_tokens(df):
    # print(df.columns)
    tokenizer = Tokenizer(num_words=MAX_NB_WORDS, filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n', lower=True)
    # tokenizer = Tokenizer(num_words=MAX_NB_WORDS, filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~', lower=True)
    tokenizer.fit_on_texts(df['sentence'].values)
    word_index = tokenizer.word_index
    print('Found %s unique tokens.' % len(word_index))


    tokenizer_json = tokenizer.to_json()
    # with open('models_covid/cnn_tokenizer_covid.json', 'w', encoding='utf-8') as f:
    #     f.write(json.dumps(tokenizer_json, ensure_ascii=False))

    return tokenizer, tokenizer_json


# with open('models_covid/cnn_tokenizer_covid.pickle', 'wb') as handle:
#     pickle.dump(tokenizer, handle, protocol=pickle.HIGHEST_PROTOCOL)


# def transform_X(X_text, tokenizer):
#     X = tokenizer.texts_to_sequences(X_text)
#     X = sequence.pad_sequences(X, maxlen=MAX_SEQUENCE_LENGTH, padding='post', truncating='post')
#     return X

def build_train_set(df, tokenizer, tags):
    print('MAX_SEQUENCE_LENGTH',MAX_SEQUENCE_LENGTH)
    X = tokenizer.texts_to_sequences(df['sentence'].values)
    X = sequence.pad_sequences(X, maxlen=MAX_SEQUENCE_LENGTH, padding='post', truncating='post')
    print('Shape of data tensor:', X.shape)


    Y = df[tags].values
    # Y = df['label'].values.reshape(-1,1)
    print('Shape of label tensor:', Y.shape)


    sent_train, sent_test, X_train, X_test,  Y_train, Y_test = train_test_split(df['sentence'].values, X,  Y, test_size = 0.10, random_state = 42, shuffle=True)
    print(X_train.shape,Y_train.shape)
    print(X_test.shape,Y_test.shape)

    sent_train, sent_val, X_train, X_val, Y_train, Y_val = train_test_split(sent_train, X_train,  Y_train, test_size = 0.10, random_state = 42, shuffle=True)

    return sent_train, sent_val, sent_test, X_train, X_val,  X_test,   Y_train, Y_val,  Y_test


def cnn():
    model = Sequential()
    # model.add(layers.Embedding(input_dim=vocab_size,
    #                            output_dim=embedding_dim,
    #                            input_length=maxlen))
    # model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1],trainable=True,mask_zero=True, weights=[embedding_matrix]))
    # model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1],trainable=True, weights=[embedding_matrix]))
    model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=MAX_SEQUENCE_LENGTH,trainable=True))
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


def train_model(model, X_train, Y_train,  X_val, Y_val,class_weight):
    model.compile(loss='binary_crossentropy', optimizer='adam',
                metrics=[
                        #  tf.keras.metrics.categorical_accuracy,
                tf.keras.metrics.AUC(name='auc')
                , tf.keras.metrics.Precision(name='precision'), tf.keras.metrics.Recall(name='recall')
                # , tf.keras.metrics.binary_crossentropy
                , tf.keras.metrics.BinaryCrossentropy(name='entropy')
                , tf.keras.metrics.TruePositives(name='tp')
                , tf.keras.metrics.TrueNegatives(name='tn')
                , tf.keras.metrics.FalsePositives(name='fp')
                , tf.keras.metrics.FalseNegatives(name='fn')
                ])


    epochs = 20
    batch_size = 64

    history = model.fit(X_train, Y_train, epochs=epochs,
                        # class_weight='balanced', #
                        class_weight = {0:1,1:class_weight},
                        batch_size=batch_size,
                        # validation_split=0.1,
                        validation_data=(X_val, Y_val),
                        callbacks=[EarlyStopping(monitor='val_auc', patience=3, min_delta=0.0001, mode='max', restore_best_weights=True)]
                        # callbacks=[EarlyStopping(monitor='val_loss', patience=3, min_delta=0.0001, mode='min', restore_best_weights=True)]
                        # callbacks=[tensorboard_callback]
                        )

    return model


def buid_models_for_tags(tags, sent_test, X_train, X_val,Y_train, Y_val, X_test, Y_test, model_path):
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
      if 1==1:
        print('-------------'*5)
        print('model for '+j )
        i = tags.index(j)


        Y_train_tag = Y_train[:,i]
        print('check y', Y_train_tag.sum(), Y_train_tag.shape)
        Y_test_tag = Y_test[:,i]
        Y_val_tag = Y_val[:,i]
        class_size = (sum(Y_test[:,i]==1)+ sum(Y_train[:,i]==1), (sum(Y_test[:,i]==1)+ sum(Y_train[:,i]==1)) / (Y_test.shape[0] + Y_train.shape[0]))
        class_weight = (sum(Y_test[:,i]==0)+ sum(Y_train[:,i]==0))/ (sum(Y_test[:,i]==1)+ sum(Y_train[:,i]==1))
        # class_weight = (sum(Y_test[:,tag_i]==0)+ sum(Y_train[:,tag_i]==0))/ (sum(Y_test[:,tag_i]==1)+ sum(Y_train[:,tag_i]==1))

        # print('X_train', X_train.shape, 'ddddd')
        m = cnn()
        m = train_model(m, X_train, Y_train_tag, X_val, Y_val_tag,class_weight)
        # m.save("./models_covid/"+'_'.join(j.split(' ')))
        m.save(model_path + '_'.join(j.split(' ')))

        data_to_evaluate = X_test
        tag_to_evaluate = Y_test_tag
        sent_to_evaluate = sent_test
        # data_to_evaluate = X_val
        # tag_to_evaluate = Y_val_tag
        # sent_to_evalue = sent_val

        metrics = m.evaluate(data_to_evaluate,tag_to_evaluate,  batch_size=tag_to_evaluate.shape[0])


        test_predictions_prob_tag = m.predict(data_to_evaluate)
        test_predictions_class_tag = (test_predictions_prob_tag>0.5)+0
        Y_test_predict[:, i] = np.concatenate((test_predictions_class_tag))
        Y_test_predict_prob[:, i] = np.concatenate((test_predictions_prob_tag))

        precisions, recalls, f1, accuracy, auc = report_metric_per_model(tag_to_evaluate, test_predictions_class_tag, average_type = 'binary')

        tag_precisions[tags[i]] = precisions
        tag_recalls[tags[i]] = recalls
        tag_f1[tags[i]] = f1
        tag_accuracy[tags[i]] = accuracy
        tag_auc[tags[i]] = auc
        tag_size[tags[i]] = class_size

    metric_df = report_metrics_for_all(tag_size, tag_precisions, tag_recalls, tag_f1, tag_accuracy, tag_auc)
    metric_df.to_csv(model_path+'/models_metrics_cnn.csv', index=True)


    actual = []
    predict = []
    sentence = []

    for i in np.arange(0,sent_to_evaluate.shape[0]):
        sentence.append(sent_to_evaluate[i])
        actual.append(list(compress(tags, Y_test[i])))
        predict.append(list(compress(tags, Y_test_predict_prob[i]>0.5)))

    return metric_df



def build_models_pipeline(**context):
    print('tags_general', tags_general)
    print('tags_covid', tags_covid)
    if run_mode=='prod':
        df = context['task_instance'].xcom_pull(task_ids='fetch-interaction-data')
    elif run_mode=='dev':
        df = pd.read_csv(context['data'])
        df['id'] = 'placeholder'

    if len(tags_general)>0:
        tokenizer_general, tokenizer_json_general = build_tokens(df)
        sent_train, sent_val, sent_test, X_train, X_val, X_test, Y_train, Y_val, Y_test = build_train_set(df, tokenizer_general, tags_general)
        metric_df_general = buid_models_for_tags(tags_general,  sent_test, X_train, X_val, Y_train, Y_val, X_test, Y_test,
                                                 model_path = "models/models_general/")


    if len(tags_covid)>0:
        df_covid = df[df['Covid-19'] > 0][['sentence'] + tags_covid]
        tokenizer_covid, tokenizer_json_covid = build_tokens(df_covid)
        sent_train, sent_val, sent_test, X_train, X_val,  X_test,   Y_train, Y_val,  Y_test = build_train_set(df_covid, tokenizer_covid, tags_covid)
        metric_df_covid = buid_models_for_tags(tags_covid,  sent_test, X_train, X_val, Y_train, Y_val, X_test, Y_test,
                                               model_path = "models/models_covid/")


    # return metric_df_general, metric_df_covid


# run_mode='dev'
# build_models_pipeline(data='to_train.csv')


# df = pd.read('interaction_all_data_0811.csv')


def save_model():
    bucket = config.S3_IMPORT_DATA_BUCKET

    today = datetime.date.today()
    today = today.strftime("%Y%m%d")
    shutil.make_archive('models_'+today, 'zip', 'models')

    s3 = boto3.client('s3', region_name='eu-west-2')
    s3.upload_file('models_'+today+'.zip',
                   bucket,
                 'models/data_hub_policy_feedback_tags_classifier/'+'models_'+today+'.zip')

    return None



# def transform_X(X_text, tokenizer):
#     X = tokenizer.texts_to_sequences(X_text)
#     X = sequence.pad_sequences(X, maxlen=MAX_SEQUENCE_LENGTH, padding='post', truncating='post')
#     return X
#
#
# # def fetch_model():
# #     bucket = config.S3_IMPORT_DATA_BUCKET
# #
# #     # print('print bucket', bucket)
# #     # print(os.environ.get('AWS_ACCESS_KEY_ID'))
# #     # print(os.environ.get('AWS_SECRET_ACCESS_KEY'))
# #     # AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
# #     # AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY.replace('%2B', '+').replace('%2F', '/')
# #     # AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
# #     # print('print key', AWS_ACCESS_KEY_ID)
# #     # print('print key',AWS_SECRET_ACCESS_KEY)
# #     # s3 = boto3.client('s3', region_name='eu-west-2', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
# #     # s3 = boto3.client('s3', region_name='eu-west-2', aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
# #
# #     s3 = boto3.client('s3', region_name='eu-west-2')
# #     s3.download_file(bucket, 'models/data_hub_policy_feedback_tags_classifier/models.zip', 'models.zip')
# #     with zipfile.ZipFile("models.zip","r") as zip_ref:
# #         zip_ref.extractall()
#
#
# def _predict(X_to_predict, tokenizer, tags_to_predcit, model_path):
#     print('i am predicting')
#     # text_to_predict = X_to_predict.copy()
#
#     ids = X_to_predict['id']
#     X_to_predict = X_to_predict['sentence']
#     text_to_predict = X_to_predict.copy()
#
#     X_to_predict = transform_X(X_to_predict.values, tokenizer)
#     Y_test_predict = np.zeros((X_to_predict.shape[0], len(tags_to_predcit)))
#     Y_test_predict_prob = np.zeros((X_to_predict.shape[0], len(tags_to_predcit)))
#     # print('61', Y_test_predict_prob)
#
#     for ind,tag_i in enumerate(['_'.join(j.split(' ')) for j in tags_to_predcit]):
#         print('yayayayya', ind, tag_i)
#               # m = tf.keras.models.load_model("../train/models_covid/"+tag_i)
#         m = tf.keras.models.load_model(model_path+tag_i)
#         # print('62', m)
#         test_predictions_prob_tag = m.predict(X_to_predict)
#         # print('63', test_predictions_prob_tag)
#         test_predictions_class_tag = (test_predictions_prob_tag>0.5)+0
#         Y_test_predict_prob[:, ind] = np.concatenate((test_predictions_prob_tag))
#         Y_test_predict[:, ind] = np.concatenate((test_predictions_class_tag))
#         # print(ind, tag_i, test_predictions_prob_tag)
#         # print('64', Y_test_predict)
#
#
#     predict = []
#     sentence = []
#     predict_prob = []
#
#
#     for i in np.arange(0,X_to_predict.shape[0]):
#         sentence.append(X_to_predict[i])
#         predict.append(list(compress(tags_to_predcit, Y_test_predict_prob[i]>0.5)))
#         predict_prob.append(dict(zip(tags_to_predcit, Y_test_predict_prob[i])))
#
#
#     prediction_on_data = pd.DataFrame({'id': ids, 'sentence':text_to_predict,  'prediction': predict, 'prediction_prob': predict_prob})
#
#     # print('6666', prediction_on_data)
#     return prediction_on_data
#
#
# def update_prediction_dict(d1,d2):
#     d = d1.copy()
#     if not isinstance(d1, float) and not isinstance(d2, float) :
#         d.update(d2)
#     return d
#
#
# def top_5_labels(dict_of_probs, threshold=0.5):
#     dict_of_probs_final = dict_of_probs.copy()
#     dict_of_probs_final = {k:v for k,v in dict_of_probs_final.items() if v>threshold}
#     covid_tags = [i for i in dict_of_probs_final.keys() if i.lower().startswith('covid')]
#     if len(covid_tags)==1:
#         dict_of_probs_final = {k.replace('COVID-19', 'Covid-19 General'):v for k,v in dict_of_probs_final.items()}
#     elif len(covid_tags)>1:
#         dict_of_probs_final.pop('COVID-19', None)
#     dict_of_probs_final = dict(sorted(dict_of_probs_final.items(), key=operator.itemgetter(1), reverse=True)[:5])
#     if isinstance(dict_of_probs_final, float) or len(dict_of_probs_final)==0:
#         dict_of_probs_final = {'General': 0}
#     return dict_of_probs_final
#
#
# def write_prediction(table_name, **context):
#     df = context['task_instance'].xcom_pull(task_ids='predict-tags')
#     df_json = df.to_json(orient="records")
#     df_json = json.loads(df_json)
#
#
#     s3 = S3Data(table_name, context["ts_nodash"])
#     s3.write_key('tags_prediction.json', df_json)
#
#
# def predict_tags(model_path, **context):
#     # memory_used0 = memory_profiler.memory_usage()
#
#     with open(model_path + '/models_general/cnn_tokenizer_general.json') as f:
#         data_general = json.load(f)
#         tokenizer_general = tokenizer_from_json(data_general)
#
#     with open(model_path + '/models_covid/cnn_tokenizer_covid.json') as f:
#         data_covid = json.load(f)
#         tokenizer_covid = tokenizer_from_json(data_covid)
#
#     df = context['task_instance'].xcom_pull(task_ids='fetch-interaction-data')
#
#     # print('----------')
#     # print(df.columns)
#     # print(df.head(5))
#     # print('11111',tokenizer_covid)
#     # print(model_path + '/models_covid/')
#     # prediction_on_general_data = _predict(X_to_predict=df['sentence'], tokenizer=tokenizer_general,
#     #                                      tags_to_predcit=tags_genearl, model_path=model_path + '/models_general/')
#     # prediction_on_covid_data = _predict(X_to_predict=covid_to_predict['sentence'], tokenizer=tokenizer_covid,
#     #                                    tags_to_predcit=tags_covid, model_path=model_path + '/models_covid/')
#     # predictions = prediction_on_general_data.merge(prediction_on_covid_data, left_index=True, right_index=True,
#     #                                                how='left')
#
#     prediction_on_general_data = _predict(X_to_predict=df, tokenizer=tokenizer_general,
#                                          tags_to_predcit=tags_genearl, model_path=model_path + '/models_general/')
#
#     covid_to_predict = prediction_on_general_data[
#         prediction_on_general_data['prediction'].apply(lambda x: 'COVID-19' in x)]
#
#     prediction_on_covid_data = _predict(X_to_predict=covid_to_predict[['id', 'sentence']], tokenizer=tokenizer_covid,
#                                        tags_to_predcit=tags_covid, model_path=model_path + '/models_covid/')
#
#     predictions = prediction_on_general_data.merge(prediction_on_covid_data, left_on='id', right_on='id',
#                                                    how='left')
#
#     predictions['prediction_prob'] = predictions.apply(
#         lambda x: update_prediction_dict(x['prediction_prob_x'], x['prediction_prob_y']), axis=1)
#     predictions['prediction_prob_top_5'] = predictions.apply(
#         lambda x: top_5_labels(x['prediction_prob'], threshold=0.5), axis=1)
#     predictions['tags_prediction'] = predictions.apply(lambda x: list(x['prediction_prob_top_5'].keys()), axis=1)
#     predictions['tags_prediction'] = predictions['tags_prediction'].apply(lambda x: ','.join(x))
#
#     del predictions['sentence_y']
#     predictions = predictions.rename(columns={'sentence_x': 'sentence'})
#
#
#
#     # memory_used = memory_profiler.memory_usage()
#     # print('memory used', memory_used[0] - memory_used0[0])
#
#     predictions = predictions[['id', 'sentence', 'tags_prediction']]
#     predictions = predictions.rename(columns = {'sentence': 'policy_feedback_notes'})
#
#     # predictions.to_csv('to_predict_result.csv', index=False)
#     return predictions
#
#
#
#
#
#
