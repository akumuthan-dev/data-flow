import pandas as pd
import re
from dataflow.utils import logger
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)


def decontracted(phrase):
    phrase = re.sub(r"won't", "will not", phrase)
    phrase = re.sub(r"can\'t", "can not", phrase)
    phrase = re.sub(r"coronavirus", "covid", phrase)
    phrase = re.sub(r"corona virus", "covid", phrase)
    phrase = re.sub(r'https?:.*?(?=$|\s)', '', phrase, flags=re.MULTILINE)

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
    excluded_list = [
        'see notes above',
        'see email above',
        'see notes box above',
        'see "as above"',
        'none',
        'feedback as above',
        '"As above"',
        'see notes',
        'see email in notes',
        'covid-19',
        'covid 19',
        'covis 19',
        'refer to above notes',
        'see email details above',
        'see email detail above',
        'included in notes above',
        'please see above',
        'cbils',
        'feedback in above notes',
        'see interaction notes',
        '',
        'no additional notes',
        'refer to above notes',
        'please see the notes above',
    ]
    excluded_list_dot = [i + '.' for i in excluded_list]
    excluded_list.extend(excluded_list_dot)
    fb_all = fb_all[fb_all['policy feedback'].str.len() > 25]
    fb_all = fb_all[~fb_all['policy feedback'].isin(excluded_list)]
    fb_all = fb_all[
        ~fb_all['policy feedback']
        .str.lower()
        .str.startswith('file:///c:/users/nick.neal/appdata/')
    ]
    fb_all = fb_all[
        ~fb_all['policy feedback'].str.lower().str.startswith('see detail above')
    ]
    fb_all = fb_all[
        ~fb_all['policy feedback'].str.lower().str.startswith('detail above')
    ]
    fb_all = fb_all[~fb_all['policy feedback'].str.lower().str.startswith('see above')]

    return fb_all


def find_text(y, column_name='policy feedback'):
    text = y[column_name].lower()
    text_list = re.split(r'\W+', text)
    return text, text_list


def relabel(x):
    x = x.copy()

    if 'policy_issue_types' in x.columns and 'Transition Period - General' in x.columns:
        x['Transition Period - General'] = x.apply(
            lambda row: 1
            if row['policy_issue_types'] == '{"EU exit"}'
            else row['Transition Period - General'],
            axis=1,
        )

    if 'Covid-19' in x.columns:
        x['Covid-19'] = x.apply(
            lambda row: 1
            if any(i in find_text(row)[1] for i in ['covid'])
            else row['Covid-19'],
            axis=1,
        )

    if 'Covid-19 Employment' in x.columns:
        x['Covid-19 Employment'] = x.apply(
            lambda row: 1
            # 'employment' removed from the list: it can be related to non-covid-19 issues after the pandemic
            if any(
                # i in find_text(row)[1] for i in ['employment', 'furlough', 'furloughed']
                i in find_text(row)[1]
                for i in ['furlough', 'furloughed']
            )
            else row['Covid-19 Employment'],
            axis=1,
        )

    if 'Exports/Imports' in x.columns:
        x['Exports/Imports'] = x.apply(
            lambda row: 1
            if any(
                i in find_text(row)[1]
                for i in ['export', 'import', 'exports', 'imports']
            )
            else row['Exports/Imports'],
            axis=1,
        )

    if 'Covid-19 Supply Chain/Stock' in x.columns:
        x['Covid-19 Supply Chain/Stock'] = x.apply(
            lambda row: 1
            if any(i in find_text(row)[0] for i in ['supply chain'])
            else row['Covid-19 Supply Chain/Stock'],
            axis=1,
        )

    if 'Cashflow' in x.columns:
        x['Cashflow'] = x.apply(
            lambda row: 1
            if any(i in find_text(row)[1] for i in ['cashflow', 'cash'])
            or 'cash flow' in find_text(row)[0]
            else row['Cashflow'],
            axis=1,
        )

    if 'Migration And Immigration' in x.columns:
        x['Migration And Immigration'] = x.apply(
            lambda row: 1
            if any(i in find_text(row)[1] for i in ['migration', 'immigration'])
            else row['Migration And Immigration'],
            axis=1,
        )

    if 'Tax' in x.columns:
        x['Tax'] = x.apply(
            lambda row: 1
            if any(i in find_text(row)[1] for i in ['tax'])
            else row['Tax'],
            axis=1,
        )

    if 'Free Trade Agreements' in x.columns:
        x['Free Trade Agreements'] = x.apply(
            lambda row: 1
            if any(
                i in find_text(row)[0] for i in ['trade agreement', 'trade agreements']
            )
            or 'fta' in find_text(row)[1]
            else row['Free Trade Agreements'],
            axis=1,
        )

    if 'Investment' in x.columns:
        x['Investment'] = x.apply(
            lambda row: 1
            if any(i in find_text(row)[1] for i in ['investment'])
            else row['Investment'],
            axis=1,
        )

    if 'Regulation' in x.columns:
        x['Regulation'] = x.apply(
            lambda row: 1
            if any(i in find_text(row)[1] for i in ['regulation', 'regulations'])
            else row['Regulation'],
            axis=1,
        )

    if 'Supply Chain' in x.columns:
        x['Supply chain'] = x.apply(
            lambda row: 1
            if any(i in find_text(row)[0] for i in ['supply chain'])
            else row['Supply Chain'],
            axis=1,
        )

    if 'Transition Period - General' in x.columns:
        x['Transition Period - General'] = x.apply(
            lambda row: 1
            if any(i in find_text(row)[0] for i in ['eu exit'])
            or 'brexit' in find_text(row)[1]
            else row['Transition Period - General'],
            axis=1,
        )

    if 'Opportunities' in x.columns:
        x['Opportunities'] = x.apply(
            lambda row: 1
            if any(i in find_text(row)[1] for i in ['opportunities', 'opportunity'])
            else row['Opportunities'],
            axis=1,
        )

    return x


def clean_tag(df):
    replace_map = {
        'Covid-19 Expectations': 'Covid-19 Future Expectations',
        'Covid-19 Hmg Support': 'HMG support request',
        # 'Covid-19 Exports': 'Covid-19 Exports/Imports', 'Covid-19 Imports': 'Covid-19 Exports/Imports',
        'Covid-19 Resuming Business': 'Covid-19 Resuming Operations',
        'Opportunities': 'Opportunities',
        'Exports - other': 'Exports',
        'Export': 'Exports',
        'Cash Flow': 'Cashflow',
        'Opportunity': 'Opportunities',
        'Opportunities\u200b': 'Opportunities',
        # 'Migration and Immigration': 'Movement of people',
        'Future Expectations': 'Expectations',
        'Border arrangements\u200b': 'Border arrangements',
        'Licencing\u200b': 'Licencing',
        'Licencing\xa0\u200b': 'Licencing',
        'Border\xa0arrangements': 'Border arrangements',
        'Border\xa0arrangements\u200b': 'Border arrangements',
        'Stock\xa0\u200b': 'Stock',
        'EU Exit - General': 'Transition Period - General',
        'EU Exit': 'Transition Period - General',
        'Post-transition Period - General': 'Transition Period - General',
        'Transition Period - General': 'Transition Period - General',
        'Transition Period General': 'Transition Period - General',
        'HMG Comms on EU Exit': 'Transition Period - General',
        'Covid-19 Resuming Business\u200b': 'Covid-19 Resuming Operations',
        'COVID-19 Cash Flow': 'Cashflow',
        'Cashflow': 'Cashflow',
        'COVID-19 Investment': 'Investment',
        'COVID-19 Exports': 'Exports',
        'COVID-19 Imports': 'imports',
        'COVID-19 Supply Chain/Stock': 'Supply Chain',
        'COVID-19 Opportunity': 'Opportunities',
        'COVID-19 Request for HMG support/changes': 'HMG support request',
        'HMG Financial support\u200b': 'HMG Support Request',
        'Covid-19 Request For Hmg Support': 'HMG support request',
        'Export - Declining Orders': 'exports',
        'Declining Export Orders': 'exports',
        'HMG Financial Support': 'HMG Support Request',
        'Financial Support': 'HMG Support Request',
        'Hmg Financial/Business Support': 'HMG Support Request',
        'Investment Decision - Alternative': 'investment',
        'Investment Decision - Cancelled': 'investment',
        'Investment Decisions - Delayed': 'investment',
        'Licencing': 'regulation',
        'regulations': 'regulation',
        'Movement of staff/employees': 'Migration and Immigration',
        'Movement of staff': 'Migration and Immigration',
        'Movement of employees': 'Migration and Immigration',
        'Temporary Movement Of Staff/Employees': 'Migration and Immigration',
        'Movement Of Goods': 'Movement Of Goods/Services',
    }

    replace_map = {k.title(): v.title() for k, v in replace_map.items()}

    removed_tags = [
        i.lower()
        for i in [
            'COVID-19 Offers of support',
            'COVID-19 DIT delivering for HMG',
            'Reduced Profit',
        ]
    ]
    df['tags'] = df['tags'].apply(
        lambda x: [
            replace_map.get(i.strip().title(), i.strip().title())
            for i in x.split(',')
            if i.lower() not in removed_tags
        ]
    )
    df['tags'] = df['tags'].apply(lambda x: ','.join(x))

    return df


def preprocess(fb_all, action='train'):

    fb_all = fb_all.rename(
        columns={
            'Policy Feedback Notes': 'policy feedback',
            'Biu Issue Types': 'tags',
            'biu_issue_type': 'tags',
            'policy_feedback_notes': 'policy feedback',
        }
    )
    fb_all = fb_all.dropna(subset=['policy feedback'])

    fb_all = exclude_notes(fb_all)
    fb_all['policy feedback'] = fb_all['policy feedback'].apply(
        lambda x: decontracted(x)
    )
    fb_all['length'] = fb_all['policy feedback'].str.len()
    fb_all = fb_all[fb_all['length'] > 25]
    fb_all = fb_all.reset_index(drop=True)

    if action == 'train':
        fb_all = fb_all[fb_all['tags'] != 'Not Specified']
        fb_all = fb_all[['id', 'policy feedback', 'tags']]
        fb_all = fb_all.dropna(subset=['policy feedback', 'tags'])
        fb_all['tags'] = fb_all['tags'].apply(
            lambda x: x.replace(';', ',')
            .replace('\u200b', '')
            .replace('â€‹', '')
            .replace('Â', '')
        )
        fb_all['tags'] = fb_all['tags'].apply(
            lambda x: x + ',covid-19'.title() if 'covid' in x.lower() else x
        )

        fb_all = clean_tag(fb_all)

        # fb_all['tags'] = fb_all['tags'].apply(
        #     lambda x: x + ',exports/imports'.title()
        #     if 'exports' in x.lower() or 'imports' in x.lower()
        #     else x
        # )

        fb_tag = fb_all['tags'].str.strip().str.get_dummies(sep=',')

        tags_count = fb_tag.sum().sort_values(ascending=False)
        fb_tag.columns = [i.strip().title() for i in fb_tag.columns]
        df = fb_all.merge(fb_tag, left_index=True, right_index=True)
        df = relabel(df)

        tags_200 = list(tags_count[tags_count > 200].index)
        tags_200 = [
            i
            for i in tags_200
            if i.lower()
            not in ['general', 'not specified', 'other', 'others', 'covid-19 general']
        ]
        # tags = kwargs['tags']
        logger.info(f'tags counts: {tags_count}')
        logger.info(f'ordered tags counts: {tags_count.sort_index()}')
        logger.info(f'tags with more than 200 counts: {tags_200}')
        logger.info(f'train model for these tags (more than 200 samples): {tags_200}')

        select_columns = ['id', 'policy feedback']
        select_columns.extend(tags_200)
        if 'bert_vec_cleaned' in fb_all.columns:
            select_columns.append('bert_vec_cleaned')

        df = df[select_columns]

    if action == 'predict':
        df = fb_all[['id', 'policy feedback']]
        tags_200 = None

    df = df.rename(columns={'policy feedback': 'sentence'})
    df = df.dropna()

    return df, tags_200


def report_metric_per_model(actual, predict, average_type='binary'):
    precisions = precision_score(actual, predict, average=average_type)
    recalls = recall_score(actual, predict, average=average_type)
    f1 = f1_score(actual, predict, average=average_type)
    accuracy = accuracy_score(actual, predict)
    auc = roc_auc_score(actual, predict)
    logger.info(f"Precision = {precisions}")
    logger.info(f"Recall = {recalls}")
    logger.info(f"f1 = {f1}")
    logger.info(f"Accuracy = {accuracy}")
    # logger.info("AUC = {}".format(roc_auc_score(Y_test_tag, np.concatenate(test_predictions_tag))))
    logger.info(f"AUC = {auc}")

    return precisions, recalls, f1, accuracy, auc


def report_metrics_for_all(
    tag_size, tag_precisions, tag_recalls, tag_f1, tag_accuracy, tag_auc
):
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

    metric_df = pd.concat(
        [size_df, precisions_df, recalls_df, f1_df, accuracy_df, auc_df], axis=1
    )
    metric_df['model_for_tag'] = metric_df.index
    metric_df = metric_df[['model_for_tag'] + list(metric_df.columns[:-1])]

    return metric_df
