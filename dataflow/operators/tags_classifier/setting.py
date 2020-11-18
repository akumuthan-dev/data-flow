all_tags = [
    'Covid-19',
    'Covid-19 Feedback On Hmg Support',
    'Covid-19 Business Disruption',
    'Exports',
    'Investment',
    'Hmg Support Request',
    'Covid-19 Employment',
    'Transition Period - General',
    'Opportunities',
    # 'Exports/Imports',
    'Covid-19 Resuming Operations',
    'Cashflow',
    'Covid-19 Future Expectations',
    'Regulation',
    'Tariffs',
    'Covid-19 Supply Chain/Stock',
    'Free Trade Agreements',
    'Skills',
    'Migration And Immigration',
    'Access To Finance',
    'Tax',
    'Border Arrangements',
]

tags_covid = [
    i for i in all_tags if i.lower().startswith('covid') and i.lower() != 'covid-19'
]

tags_general = [i for i in all_tags if i not in tags_covid]

MAX_NB_WORDS = 10000
MAX_SEQUENCE_LENGTH = 500
EMBEDDING_DIM = 100
model_version = 'models_20201029.zip'
probability_threshold = 0.4
training_data_file = 'training_data_20201029.csv'
