import numpy
from tensorflow.keras.datasets import imdb
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense ##if only from keras.layers import Dense, then inaccurate calclations on tp, tn, np, fn etc
from tensorflow.keras.layers import LSTM, Bidirectional
from tensorflow.keras.layers import Embedding, Conv1D, GlobalMaxPooling1D, Flatten
# from keras.layers.embeddings import Embedding
from tensorflow.keras.preprocessing import sequence
# fix random seed for reproducibility
numpy.random.seed(7)
import tensorflow as tf

tf.random.set_seed(2)
from tensorflow.keras.preprocessing.text import Tokenizer
from sklearn.model_selection import train_test_split
# from keras.callbacks import EarlyStopping, TensorBoard
from tensorflow.keras.callbacks import EarlyStopping, TensorBoard
import os
import datetime
import pandas as pd
import numpy as np
from tensorflow.keras import layers
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, f1_score, roc_auc_score, roc_curve,auc
# from sklearn import metrics
from scipy import stats
import pickle
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import re
from itertools import compress
from dataflow.operators.tags_classifier_train.setting import tags_covid, tags_general

select_tags = tags_general
tags = select_tags

fb_all = pd.read_csv("data_for_lingling_select_interaction_all_data_0819.csv", delimiter=',')
fb_all = pd.read_csv('training_data_0901.csv', delimiter=',')


fb_all = fb_all[['policy_feedback_notes', 'biu_issue_type']]
fb_all = fb_all.rename(columns={'policy_feedback_notes': 'policy feedback', 'biu_issue_type': 'tags'})
fb_all['tags'] = fb_all['tags'].apply(lambda x: x.replace(';', ','))

#check
fb_all = fb_all.dropna(subset=['policy feedback'])
print('check 1', fb_all.shape)


#
#
def clean_tag(df):
    replace_map = {'Covid-19 Expectations': 'Covid-19 Future Expectations',
                   'Covid-19 Hmg Support': 'Covid-19 Request For Hmg Support'
                   #,'Covid-19 Exports': 'Covid-19 Exports/Imports', 'Covid-19 Imports': 'Covid-19 Exports/Imports'
                   ,'Covid-19 Resuming Business': 'Covid-19 Resuming Operations',
                   'Opportunities': 'Opportunities',
                   'Exports - other': 'Export',
                   'Cash Flow': 'Cashflow',
                   'Opportunity': 'Opportunities', 'Opportunities\u200b': 'Opportunities',
                   'Migration and Immigration': 'Movement of people',
                   'Future Expectations': 'Expectations',
                   'Border arrangements\u200b': 'Border arrangements',
                   'Licencing\u200b': 'Licencing', 'Licencing\xa0\u200b': 'Licencing',
                   'Border\xa0arrangements': 'Border arrangements',
                   'Border\xa0arrangements\u200b': 'Border arrangements',
                   'Stock\xa0\u200b': 'Stock',
                   'EU Exit - General': 'EU Exit',
                   'Post-transition Period - General': 'EU Exit',
                   'Transition Period - General': 'EU Exit',
                   'HMG Comms on EU Exit': 'EU Exit', 'HMG Financial support\u200b': 'HMG Financial support'
                   }
    # replace_map = {k.lower():v.lower() for k,v in replace_map.items()}
    #
    # df['tags'] = df['tags'].apply(lambda x: [replace_map.get(i.lower(), i.lower()) for i in x.split(',')])
    # df['tags'] = df['tags'].apply(lambda x: ','.join(x))

    df['tags'] = df['tags'].apply(lambda x: [replace_map.get(i.strip(), i.strip()) for i in x.split(',')])
    df['tags'] = df['tags'].apply(lambda x: ','.join(x))

    return df
# fb_all = clean_tag(fb_all)
#
#




replace_map = {
    'Migration and Immigration': 'Movement of people',
    'Opportunities': 'Opportunities',
    'Exports - other': 'Export',
    'Cash Flow': 'Cashflow',
    'Opportunity': 'Opportunities', 'Opportunities\u200b': 'Opportunities',
    'Migration and Immigration': 'Movement of people',
    'Future Expectations': 'Expectations',
    'Border arrangements\u200b': 'Border arrangements',
    'Licencing\u200b': 'Licencing', 'Licencing\xa0\u200b': 'Licencing',
    'Border\xa0arrangements': 'Border arrangements', 'Border\xa0arrangements\u200b': 'Border arrangements',
    'Stock\xa0\u200b': 'Stock',
    'EU Exit - General': 'EU Exit',
    'Post-transition Period - General': 'EU Exit',
    'Transition Period - General': 'EU Exit',
    'HMG Comms on EU Exit': 'EU Exit', 'HMG Financial support\u200b': 'HMG Financial support'
}

replace_map = {'Covid-19 Expectations': 'Covid-19 Future Expectations',
               'Covid-19 Hmg Support': 'Covid-19 Request For Hmg Support'
               # ,'Covid-19 Exports': 'Covid-19 Exports/Imports', 'Covid-19 Imports': 'Covid-19 Exports/Imports'
               , 'Covid-19 Resuming Business': 'Covid-19 Resuming Operations',
               'Opportunities': 'Opportunities',
               'Exports - other': 'Export',
               'Cash Flow': 'Cashflow',
               'Opportunity': 'Opportunities', 'Opportunities\u200b': 'Opportunities',
               'Migration and Immigration': 'Movement of people',
               'Future Expectations': 'Expectations',
               'Border arrangements\u200b': 'Border arrangements',
               'Licencing\u200b': 'Licencing', 'Licencing\xa0\u200b': 'Licencing',
               'Border\xa0arrangements': 'Border arrangements',
               'Border\xa0arrangements\u200b': 'Border arrangements',
               'Stock\xa0\u200b': 'Stock',
               'EU Exit - General': 'EU Exit',
               'Post-transition Period - General': 'EU Exit',
               'Transition Period - General': 'EU Exit',
               'HMG Comms on EU Exit': 'EU Exit', 'HMG Financial support\u200b': 'HMG Financial support'
               }



fb_all['tags'] = fb_all['tags'].apply(lambda x: x.split(','))
fb_all['tags'] = fb_all['tags'].apply(lambda x: ['Covid-19' if 'covid' in i.lower() else i for i in x])
fb_all['tags'] = fb_all['tags'].apply(lambda x: [replace_map.get(i.strip(), i.strip()) for i in x])
fb_all['tags'] = fb_all['tags'].apply(lambda x: ','.join(x))


# fb_all = clean_tag(fb_all)


fb_all = fb_all.dropna(subset=['policy feedback', 'tags'])
fb_all.head(1)
fb_all.shape

print('check 2',fb_all.shape)


fb_tag = fb_all['tags'].str.strip().str.get_dummies(sep=',')
fb_tag.columns = [i.strip().title() for i in fb_tag.columns]
fb_tag.head(1)

fb_all = fb_all.merge(fb_tag, left_index=True, right_index=True)

fb_all = fb_all[fb_all['tags']!='Not Specified']
fb_all = fb_all[fb_all['tags'] != 'Not Specified']
fb_all['policy feedback'] = fb_all['policy feedback'].apply(
    lambda x: re.sub(r'https?:.*?(?=$|\s)', '', x, flags=re.MULTILINE))

def exclude_notes(fb_all):
    excluded_list = ['see notes above', 'see email above', 'see notes box above', 'see "as above"', 'none',
                     'feedback as above',
                     'see email in notes', 'covid-19', 'covid 19', 'covis 19', 'refer to above notes',
                     'see email details above', 'see email detail above',
                     'included in notes above', 'please see above', 'cbils', 'feedback in above notes',
                     'see interaction notes', '',
                     'no additional notes', 'refer to above notes', 'please see the notes above']
    excluded_list_dot = [i + '.' for i in excluded_list]
    excluded_list.extend(excluded_list_dot)
    print(fb_all.columns)
    print(fb_all.head(3))
    fb_all = fb_all[fb_all['policy feedback'].str.len() > 0]
    fb_all = fb_all[~fb_all['policy feedback'].isin(excluded_list)]
    fb_all = fb_all[~fb_all['policy feedback'].str.lower().str.startswith('file:///c:/users/nick.neal/appdata/')]
    fb_all = fb_all[~fb_all['policy feedback'].str.lower().str.startswith('see detail above')]
    fb_all = fb_all[~fb_all['policy feedback'].str.lower().str.startswith('detail above')]
    fb_all = fb_all[~fb_all['policy feedback'].str.lower().str.startswith('see above')]

    return fb_all
#
# excluded_list = ['see notes above', 'see email above', 'see notes box above', 'see "as above"', 'none',
#                  'feedback as above',
#                  'see email in notes', 'covid-19', 'covid 19', 'covis 19', 'refer to above notes',
#                  'see email details above', 'see email detail above',
#                  'included in notes above', 'please see above', 'cbils', 'feedback in above notes',
#                  'see interaction notes', '',
#                  'no additional notes', 'refer to above notes', 'please see the notes above']
# excluded_list_dot = [i + '.' for i in excluded_list]
# excluded_list.extend(excluded_list_dot)
#
# fb_all = fb_all[~fb_all['policy feedback'].isin(excluded_list)]
# fb_all.shape

fb_all = exclude_notes(fb_all)


import re


def decontracted(phrase):
    # specific
    phrase = re.sub(r"won't", "will not", phrase)
    phrase = re.sub(r"can\'t", "can not", phrase)
    phrase = re.sub(r"coronavirus", "covid", phrase)
    phrase = re.sub(r"corona virus", "covid", phrase)
    phrase = re.sub(r'https?:.*?(?=$|\s)', '', phrase, flags=re.MULTILINE)
    # general
    phrase = re.sub(r"n\'t", " not", phrase)
    phrase = re.sub(r"\'re", " are", phrase)
    phrase = re.sub(r"\'s", " is", phrase)
    phrase = re.sub(r"\'d", " would", phrase)
    phrase = re.sub(r"\'ll", " will", phrase)
    phrase = re.sub(r"\'t", " not", phrase)
    phrase = re.sub(r"\'ve", " have", phrase)
    phrase = re.sub(r"\'m", " am", phrase)
    return phrase


fb_all['policy feedback'] = fb_all['policy feedback'].apply(lambda x: decontracted(x))

fb_all['length'] = fb_all['policy feedback'].str.len()
fb_all = fb_all[fb_all['length'] > 25]

# if 'Covid-19 Exports/Imports' in fb_all.columns:
#     fb_all['Covid-19 Exports/Imports'] = fb_all.apply(
#         lambda x: 1 if x['Covid-19 Exports'] == 1 or x['Covid-19 Imports'] == 1 else 0, axis=1)
#
# if 'Covid-19 Employment' in fb_all.columns:
#     tags = [i for i in fb_all.columns if i.lower().startswith('covid')]
#     tags = ['Covid-19 Employment', 'Covid-19 Cash Flow', 'Covid-19 Supply Chain/Stock', 'Covid-19 Exports/Imports',
#             'Covid-19 Feedback On Hmg Support', 'Covid-19 Request For Hmg Support', 'Covid-19 Offers Of Support',
#             'Covid-19 Business Disruption',
#             'Covid-19 Future Expectations', 'Covid-19 Opportunity']
# else:
#     tags = ['COVID-19', 'Investment', 'Tariffs', 'Regulation', 'Export', 'Skills', 'Free Trade Agreements', 'Tax']
#     # tags = ['Investment', 'Tariffs', 'Regulation', 'Export', 'Skills', 'EU Exit - General']
#     # tags = ['Free Trade Agreements','Tax']
#     # tags = ['Tariffs']
#     # tags = ['Regulation']
#     tags = ['Free Trade Agreements']
#     tags = ['Movement of people']
#     tags = select_tags




def relabel(x):
    x = x.copy()

    def find_text(x):
        text = x['policy feedback'].lower()
        text_list = re.split('\W+', text)
        return text, text_list

    if 'policy_issue_types' in x.columns and 'EU Exit' in x.columns:
        x['EU Exit'] = \
            x.apply(lambda x: 1 if x['policy_issue_types'] == '{"EU exit"}' else x['EU Exit'], axis=1)

    if 'Covid-19 Employment' in x.columns:
        x['Covid-19 Employment'] = \
            x.apply(lambda x: 1 if any(i in find_text(x)[1] for i in ['employment', 'furlough']) else x[
                'Covid-19 Employment'], axis=1)
        x['Covid-19 Exports/Imports'] = \
            x.apply(lambda x: 1 if any(i in find_text(x)[1] for i in ['export', 'import']) else x[
                'Covid-19 Exports/Imports'], axis=1)
        x['Covid-19 Supply Chain/Stock'] = \
            x.apply(lambda x: 1 if any(i in find_text(x)[0] for i in ['supply chain']) else x[
                'Covid-19 Supply Chain/Stock'], axis=1)
        x['Covid-19 Cash Flow'] = \
            x.apply(
                lambda x: 1 if any(i in find_text(x)[1] for i in ['cashflow', 'cash']) or 'cash flow' in find_text(x)[0]
                else x['Covid-19 Cash Flow'], axis=1)
    if 'Tax' in x.columns:
        x['Tax'] = \
            x.apply(lambda x: 1 if any(i in find_text(x)[1] for i in ['tax']) else x['Tax'], axis=1)
    if 'Free Trade Agreements' in x.columns:
        x['Free Trade Agreements'] = \
            x.apply(
                lambda x: 1 if any(i in find_text(x)[0] for i in ['trade agreement', 'trade agreements']) or 'fta' in
                               find_text(x)[1]
                else x['Free Trade Agreements'], axis=1)
    if 'Investment' in x.columns:
        x['Investment'] = \
            x.apply(lambda x: 1 if any(i in find_text(x)[1] for i in ['investment']) else x['Investment'], axis=1)
    if 'Regulation' in x.columns:
        x['Regulation'] = \
            x.apply(
                lambda x: 1 if any(i in find_text(x)[1] for i in ['regulation', 'regulations']) else x['Regulation'],
                axis=1)

        # if 'Stock' in x.columns:
    #     #     x['Stock'] = \
    #     #         x.apply(lambda x: 1 if any(i in find_text(x)[1] for i in ['stock']) else x[
    #     #             'Stock'], axis=1)
    if 'Supply Chain' in x.columns:
        x['Supply chain'] = \
            x.apply(lambda x: 1 if any(i in find_text(x)[0] for i in ['supply chain']) else x[
                'Supply Chain'], axis=1)

    if 'Eu Exit' in x.columns:
        x['Eu Exit'] = \
            x.apply(lambda x: 1 if any(i in find_text(x)[0] for i in ['eu exit']) or 'brexit' in find_text(x)[1]
            else x['Eu Exit'], axis=1)

    return x


select_columns = ['policy feedback', 'cleaned']
select_columns = ['policy feedback']
select_columns.extend(tags)

if 'bert_vec_cleaned' in fb_all.columns:
    select_columns.append('bert_vec_cleaned')


df = fb_all.copy()
df = relabel(df[select_columns])
# display(df[tags].sum().sort_values(ascending=False))

df = df.copy()
df = df.rename(columns={'policy feedback': 'sentence'})
df = df.dropna()

df.to_csv('to_train_check.csv', index=False)

# df= pd.read_csv('to_train_check.csv')
# df= pd.read_csv('to_train.csv')

# vectorizer = TfidfVectorizer(strip_accents='unicode', analyzer='word', ngram_range=(1,1), norm='l2')
vectorizer = CountVectorizer()
# vectors = vectorizer.fit_transform(df['cleaned'])
vectors = vectorizer.fit_transform(df['sentence'])
feature_names = vectorizer.get_feature_names()
the_index = feature_names.index('support')
df_tf = vectors.todense()
# df_tf = df_tf[:,the_index]
# denselist = dense.tolist()
# df_tf = pd.DataFrame(denselist, columns=feature_names)
print(df_tf.shape)

MAX_NB_WORDS = 10000
# Max number of words in each sentence.
MAX_SEQUENCE_LENGTH = 500
# This is fixed.
EMBEDDING_DIM = 100
tokenizer = Tokenizer(num_words=MAX_NB_WORDS, filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n', lower=True)
# tokenizer = Tokenizer(num_words=MAX_NB_WORDS, filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~', lower=True)
tokenizer.fit_on_texts(df['sentence'].values)
word_index = tokenizer.word_index
print('Found %s unique tokens.' % len(word_index))


print('check 3', df.shape)
X = tokenizer.texts_to_sequences(df['sentence'].values)
X = sequence.pad_sequences(X, maxlen=MAX_SEQUENCE_LENGTH, padding='post', truncating='post')
print('Shape of data tensor:', X.shape)


X2 = X.copy()

Y = df[tags].values
# Y = df['label'].values.reshape(-1,1)
print('Shape of label tensor:', Y.shape)

# X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size = 0.10, random_state = 42, shuffle=True)
# print(X_train.shape,Y_train.shape)
# print(X_test.shape,Y_test.shape)

# # X_train, X_val, Y_train, Y_val = train_test_split(X_train,Y_train, test_size = 0.10, random_state = 42, shuffle=True)


df_tf_train, df_tf_test, sent_train, sent_test, X_train, X_test, X2_train, X2_test, Y_train, Y_test = train_test_split(
    df_tf, df['sentence'].values, X, X2, Y, test_size=0.10, random_state=42, shuffle=True)
print(X_train.shape, Y_train.shape)
print(X_test.shape, Y_test.shape)

df_tf_train, df_tf_val, sent_train, sent_val, X_train, X_val, X2_train, X2_val, Y_train, Y_val = train_test_split(
    df_tf_train, sent_train, X_train, X2_train, Y_train, test_size=0.10, random_state=42, shuffle=True)


def lstm():
    model = Sequential()
    # model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1]))
    # model.add(Embedding(len(word_index) + 1, EMBEDDING_DIM, input_length=X.shape[1],
    #                     weights=[embedding_matrix],trainable=True))
    # model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1]
    #                     ,weights=[embedding_matrix],trainable=True))
    model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1], trainable=True, mask_zero=True))

    # # model.add(SpatialDropout1D(0.2))
    # model.add(LSTM(100, dropout=0.2, recurrent_dropout=0.2,return_sequences=True))
    # model.add(LSTM(100, dropout=0.5, recurrent_dropout=0.5))
    model.add(LSTM(32, dropout=0.2, recurrent_dropout=0.2, return_sequences=True))
    model.add(LSTM(16, dropout=0.2, recurrent_dropout=0.2))
    # model.add(Bidirectional(LSTM(64,  dropout=0.2, recurrent_dropout=0.2, return_sequences=True)))
    # model.add(Bidirectional(LSTM(32,  dropout=0.2, recurrent_dropout=0.2)))
    model.add(Dense(1, activation='sigmoid'))

    return model


def cnn():
    model = Sequential()
    # model.add(layers.Embedding(input_dim=vocab_size,
    #                            output_dim=embedding_dim,
    #                            input_length=maxlen))
    # model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1],trainable=True,mask_zero=True, weights=[embedding_matrix]))
    # model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1],trainable=True, weights=[embedding_matrix]))
    model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1], trainable=True))
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

# def logreg():
#     model = LogisticRegression(penalty="l2", C=0.05, class_weight='balanced')
#     return model
def train_model(model, X_train, Y_train,  X_val, Y_val,class_weight):
    # model = Sequential()
    # model.add(Embedding(MAX_NB_WORDS, EMBEDDING_DIM, input_length=X.shape[1],trainable=True,mask_zero=True))
    # model.add(Conv1D(64, 3, activation='sigmoid'))
    # model.add(Conv1D(100, 3, activation='sigmoid'))
    # model.add(Conv1D(100, 3, activation='sigmoid'))
    # # model.add(Dropout(0.70))
    # model.add(Conv1D(48, 3, activation='sigmoid'))
    # model.add(Flatten())
    # model.add(Dense(1))


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
#                         callbacks=[EarlyStopping(monitor='val_auc', patience=3, min_delta=0.0001, mode='max', restore_best_weights=True)]
                        callbacks=[EarlyStopping(monitor='val_entropy', patience=3, min_delta=0.0001, mode='min', restore_best_weights=True)]
                        # callbacks=[tensorboard_callback]
                        )

    return model


tag_precisions = dict()
tag_recalls = dict()
tag_f1 = dict()
tag_accuracy = dict()
tag_auc = dict()
tag_size = dict()


for j in tags:
# for j in ['Covid-19', 'Investment', 'Eu Exit']:

    i = tags.index(j)
    # if j!='Covid-19 Employment':
    #   if 1==1:
    Y_test_predict = np.zeros(Y_test.shape)
    print('-------------' * 5)
    print('model for ' + j)
    print(i)
    Y_train_tag = Y_train[:, i]
    print('check y', Y_train_tag.sum(), Y_train_tag.shape)
    Y_test_tag = Y_test[:, i]
    Y_val_tag = Y_val[:, i]
    class_size = (sum(Y_test[:, i] == 1) + sum(Y_train[:, i] == 1),
                  (sum(Y_test[:, i] == 1) + sum(Y_train[:, i] == 1)) / (Y_test.shape[0] + Y_train.shape[0]))
    class_weight = (sum(Y_test[:, i] == 0) + sum(Y_train[:, i] == 0)) / (
                sum(Y_test[:, i] == 1) + sum(Y_train[:, i] == 1))
    # class_weight = (sum(Y_test[:,tag_i]==0)+ sum(Y_train[:,tag_i]==0))/ (sum(Y_test[:,tag_i]==1)+ sum(Y_train[:,tag_i]==1))

    print(X_train.shape)
    print('class_weight', class_weight)

    m = cnn()
    m = train_model(m, X_train, Y_train_tag, X_val, Y_val_tag, class_weight)
    m.save('models_0819/' + '_'.join(j.split(' ')))

    metrics = m.evaluate(X_test, Y_test_tag, batch_size=Y_test_tag.shape[0])

    test_predictions_tag = m.predict(X_test)
    test_predictions_class_tag = (test_predictions_tag > 0.5) + 0
    Y_test_predict[:, i] = np.concatenate((test_predictions_class_tag))

    precisions = precision_score(Y_test_tag, test_predictions_class_tag, average='binary')
    recalls = recall_score(Y_test_tag, test_predictions_class_tag, average='binary')
    f1 = f1_score(Y_test_tag, test_predictions_class_tag, average='binary')
    accuracy = accuracy_score(Y_test_tag, test_predictions_class_tag)
    auc = roc_auc_score(Y_test_tag, test_predictions_tag)
    print("Precision = {}".format(precisions))
    print("Recall = {}".format(recalls))
    print("f1 = {}".format(f1))
    print("Accuracy = {}".format(accuracy))
    # print("AUC = {}".format(roc_auc_score(Y_test_tag, np.concatenate(test_predictions_tag))))
    print("AUC = {}".format(auc))

    tag_precisions[tags[i]] = precisions
    tag_recalls[tags[i]] = recalls
    tag_f1[tags[i]] = f1
    tag_accuracy[tags[i]] = accuracy
    tag_auc[tags[i]] = auc
    tag_size[tags[i]] = class_size

#
#
# def report_metric_per_model(actual, predict, average_type = 'binary'):
#     precisions = precision_score(actual, predict, average=average_type)
#     recalls = recall_score(actual, predict, average=average_type)
#     f1 = f1_score(actual, predict, average=average_type)
#     accuracy = accuracy_score(actual, predict)
#     auc = roc_auc_score(actual,predict)
#     print("Precision = {}".format(precisions))
#     print("Recall = {}".format(recalls))
#     print("f1 = {}".format(f1))
#     print("Accuracy = {}".format(accuracy))
#     # print("AUC = {}".format(roc_auc_score(Y_test_tag, np.concatenate(test_predictions_tag))))
#     print("AUC = {}".format(auc))
#
#     return precisions, recalls, f1, accuracy, auc
#
#
# Y_test_predict = np.zeros(Y_test.shape)
# for j in tags:
#     # if j!='Covid-19 Employment':
#     if 1 == 1:
#         print('-------------' * 5)
#         print('model for ' + j)
#         i = tags.index(j)
#
#         Y_train_tag = Y_train[:, i]
#         print('check y', Y_train_tag.sum(), Y_train_tag.shape)
#         Y_test_tag = Y_test[:, i]
#         Y_val_tag = Y_val[:, i]
#         class_size = (sum(Y_test[:, i] == 1) + sum(Y_train[:, i] == 1),
#                       (sum(Y_test[:, i] == 1) + sum(Y_train[:, i] == 1)) / (Y_test.shape[0] + Y_train.shape[0]))
#         class_weight = (sum(Y_test[:, i] == 0) + sum(Y_train[:, i] == 0)) / (
#                     sum(Y_test[:, i] == 1) + sum(Y_train[:, i] == 1))
#         # class_weight = (sum(Y_test[:,tag_i]==0)+ sum(Y_train[:,tag_i]==0))/ (sum(Y_test[:,tag_i]==1)+ sum(Y_train[:,tag_i]==1))
#
#         print('class_weight', class_weight)
#
#         # print('X_train', X_train.shape, 'ddddd')
#         m = cnn()
#         m = train_model(m, X_train, Y_train_tag, X_val, Y_val_tag, class_weight)
#         # m.save("./models_covid/"+'_'.join(j.split(' ')))
#
#         data_to_evaluate = X_test
#         tag_to_evaluate = Y_test_tag
#         sent_to_evaluate = sent_test
#         # data_to_evaluate = X_val
#         # tag_to_evaluate = Y_val_tag
#         # sent_to_evalue = sent_val
#
#         metrics = m.evaluate(data_to_evaluate, tag_to_evaluate, batch_size=tag_to_evaluate.shape[0])
#
#         test_predictions_prob_tag = m.predict(data_to_evaluate)
#         test_predictions_class_tag = (test_predictions_prob_tag > 0.5) + 0
#         Y_test_predict[:, i] = np.concatenate((test_predictions_class_tag))
#
#         precisions, recalls, f1, accuracy, auc = report_metric_per_model(tag_to_evaluate, test_predictions_class_tag,
#                                                                          average_type='binary')
#
#         tag_precisions[tags[i]] = precisions
#         tag_recalls[tags[i]] = recalls
#         tag_f1[tags[i]] = f1
#         tag_accuracy[tags[i]] = accuracy
#         tag_auc[tags[i]] = auc
#         tag_size[tags[i]] = class_size