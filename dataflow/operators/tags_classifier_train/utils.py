import pandas as pd
import re
from dataflow.operators.tags_classifier_train.setting import *
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, f1_score, roc_auc_score, roc_curve,auc

a=300000


######################## fetch and clean the data ########################

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

def exclude_notes(fb_all):
    excluded_list = ['see notes above', 'see email above', 'see notes box above', 'see "as above"', 'none',
                     'feedback as above', '"As above"', 'see notes'
                     'see email in notes', 'covid-19', 'covid 19', 'covis 19', 'refer to above notes',
                     'see email details above', 'see email detail above',
                     'included in notes above', 'please see above', 'cbils', 'feedback in above notes',
                     'see interaction notes', '',
                     'no additional notes', 'refer to above notes', 'please see the notes above']
    excluded_list_dot = [i + '.' for i in excluded_list]
    excluded_list.extend(excluded_list_dot)
    print(fb_all.columns)
    # print(fb_all.head(3))
    fb_all = fb_all[fb_all['policy feedback'].str.len() > 25]
    fb_all = fb_all[~fb_all['policy feedback'].isin(excluded_list)]
    fb_all = fb_all[~fb_all['policy feedback'].str.lower().str.startswith('file:///c:/users/nick.neal/appdata/')]
    fb_all = fb_all[~fb_all['policy feedback'].str.lower().str.startswith('see detail above')]
    fb_all = fb_all[~fb_all['policy feedback'].str.lower().str.startswith('detail above')]
    fb_all = fb_all[~fb_all['policy feedback'].str.lower().str.startswith('see above')]

    return fb_all



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
            x.apply(lambda x: 1 if any(i in find_text(x)[1] for i in ['employment', 'furlough', 'furloughed']) else x[
                'Covid-19 Employment'], axis=1)
    if 'Covid-19 Exports/Imports' in x.columns:
        x['Covid-19 Exports/Imports'] = \
            x.apply(lambda x: 1 if any(i in find_text(x)[1] for i in ['export', 'import']) else x[
                'Covid-19 Exports/Imports'], axis=1)
    if 'Covid-19 Supply Chain/Stock' in x.columns:
        x['Covid-19 Supply Chain/Stock'] = \
            x.apply(lambda x: 1 if any(i in find_text(x)[0] for i in ['supply chain']) else x[
                'Covid-19 Supply Chain/Stock'], axis=1)
    if 'Covid-19 Cash Flow' in x.columns:
        x['Covid-19 Cash Flow'] = \
            x.apply(lambda x: 1 if any(i in find_text(x)[1] for i in ['cashflow', 'cash']) or 'cash flow' in  find_text(x)[0]
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
                   'HMG Comms on EU Exit': 'EU Exit', 'HMG Financial support\u200b': 'HMG Financial support',
                   'Covid-19 Resuming Business\u200b': 'Covid-19 Resuming Operations'
                   }

    replace_map = {k.title():v.title() for k,v in replace_map.items()}
    # replace_map = {k.lower():v.lower() for k,v in replace_map.items()}
    # df['tags'] = df['tags'].apply(lambda x: [replace_map.get(i.lower(), i.lower()) for i in x.split(',')])

    # df['tags'] = df['tags'].apply(lambda x: [replace_map.get(i.strip(), i.strip()) for i in x.split(',')])
    df['tags'] = df['tags'].apply(lambda x: [replace_map.get(i.strip().title(), i.strip().title()) for i in x.split(',')])
    df['tags'] = df['tags'].apply(lambda x: ','.join(x))

    return df

def preprocess(fb_all, action='train', tags=all_tags):

    fb_all = fb_all.rename(columns={'Policy Feedback Notes': 'policy feedback', 'Biu Issue Types': 'tags',
                                    'biu_issue_type': 'tags',
                                    'policy_feedback_notes':'policy feedback'})
    fb_all = fb_all.dropna(subset=['policy feedback'])

    print('check 1', fb_all.shape)

    # print('yayay:', fb_all.columns)
    fb_all = exclude_notes(fb_all)
    fb_all = fb_all[fb_all['tags'] != 'Not Specified']
    fb_all['policy feedback'] = fb_all['policy feedback'].apply(lambda x: decontracted(x))
    fb_all['length'] = fb_all['policy feedback'].str.len()
    fb_all = fb_all[fb_all['length']>25]
    fb_all = fb_all.reset_index(drop=True)

    if action == 'train':
        fb_all = fb_all[['id', 'policy feedback', 'tags']]
        fb_all = fb_all.dropna(subset=['policy feedback', 'tags'])
        fb_all['tags'] = fb_all['tags'].apply(lambda x: x.replace(';', ',').replace('\u200b', '').replace('â€‹', '').replace('Â', ''))
        fb_all['tags'] = fb_all['tags'].apply(lambda x: x + ',covid-19' if 'covid' in x.lower() else x)
        fb_all = clean_tag(fb_all)
        # print(fb_all.head(1))
        fb_tag = fb_all['tags'].str.strip().str.get_dummies(sep=',')

        tags_count = fb_tag.sum().sort_values(ascending=False)
        fb_tag.columns = [i.strip().title() for i in fb_tag.columns]
        print('test', fb_tag.columns)
        # print(fb_tag.shape)
        df = fb_all.merge(fb_tag, left_index=True, right_index=True)
        # print(df.columns)
        # print('shape', fb_all.shape, fb_tag.shape, df.shape)

        # df['Covid-19 Exports/Imports'] = df.apply(
        #     lambda x: 1 if x['Covid-19 Exports'] == 1 or x['Covid-19 Imports'] == 1 else 0, axis=1)
        df = relabel(df)

        # select_columns = ['policy feedback', 'cleaned']

        # print('test', fb_tag['Movement Of People'].sum())
        tags_200 = list(tags_count[tags_count>200].index)
        tags_200 = [i for i in tags_200 if i.lower() not in ['general', 'not specified', 'other', 'others', 'covid-19 general']]
        # tags = kwargs['tags']
        print('tags counts', tags_count)
        print('tags with more than 200 counts:', tags_200)
        print('train model for these tags:', tags)


        select_columns = ['id', 'policy feedback']
        select_columns.extend(tags)
        if 'bert_vec_cleaned' in fb_all.columns:
            select_columns.append('bert_vec_cleaned')
        # print(df.columns)

        df = df[select_columns]

    if action == 'predict':
        df = fb_all[['id', 'policy feedback']]

    df = df.rename(columns= {'policy feedback': 'sentence'})
    df = df.dropna()


    return df


def report_metric_per_model(actual, predict, average_type = 'binary'):
    precisions = precision_score(actual, predict, average=average_type)
    recalls = recall_score(actual, predict, average=average_type)
    f1 = f1_score(actual, predict, average=average_type)
    accuracy = accuracy_score(actual, predict)
    auc = roc_auc_score(actual,predict)
    print("Precision = {}".format(precisions))
    print("Recall = {}".format(recalls))
    print("f1 = {}".format(f1))
    print("Accuracy = {}".format(accuracy))
    # print("AUC = {}".format(roc_auc_score(Y_test_tag, np.concatenate(test_predictions_tag))))
    print("AUC = {}".format(auc))

    return precisions, recalls, f1, accuracy, auc


def report_metrics_for_all(tag_size, tag_precisions, tag_recalls, tag_f1, tag_accuracy,tag_auc ):
    size_df = pd.DataFrame.from_dict(tag_size, orient='index')
    size_df = size_df.rename(columns={0: 'size'})
    size_df = size_df[['size']]

    precisions_df = pd.DataFrame.from_dict(tag_precisions, orient='index')
    precisions_df = precisions_df.rename(columns={0: 'precisions'})

    recalls_df = pd.DataFrame.from_dict(tag_recalls, orient='index')
    recalls_df = recalls_df.rename(columns={0: 'recalls'})

    f1_df = pd.DataFrame.from_dict(tag_f1, orient='index')
    f1_df = f1_df.rename(columns={0: 'f1'})

    accuracy_df = pd.DataFrame.from_dict(tag_accuracy, orient='index')
    accuracy_df = accuracy_df.rename(columns={0: 'accuracy'})

    auc_df = pd.DataFrame.from_dict(tag_auc, orient='index')
    auc_df = auc_df.rename(columns={0: 'auc'})

    metric_df = pd.concat([size_df, precisions_df, recalls_df, f1_df, accuracy_df, auc_df], axis=1)
    metric_df['model_for_tag'] = metric_df.index
    metric_df = metric_df[['model_for_tag'] + list(metric_df.columns[:-1])]
    # print(metric_df)
    # metric_df.columns = ['model_for_tag'] + list(metric_df.columns[1:])

    return metric_df
