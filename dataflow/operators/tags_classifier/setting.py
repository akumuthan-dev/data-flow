all_tags = [
    'Covid-19',
    'Covid-19 Feedback On Hmg Support',
    'Covid-19 Business Disruption',
    'Covid-19 Employment',
    'Covid-19 Request For Hmg Support',
    'Covid-19 Exports',
    'Covid-19 Opportunity',
    'Transition Period - General',
    'Investment',
    'Covid-19 Resuming Operations',
    'Covid-19 Cash Flow',
    'Covid-19 Future Expectations',
    'Covid-19 Offers Of Support',
    'Covid-19 Supply Chain/Stock',
    'Tariffs',
    'Export',
    'Regulation',
    'Covid-19 Investment',
    'Skills',
    'Free Trade Agreements',
    'Migration And Immigration',
    'Access To Finance',
    'Tax',
]


all_tags = ['Covid-19', 'Covid-19 Feedback On Hmg Support', 'Covid-19 Business Disruption', 'Exports', 'Investment', 'Hmg Support Request', 'Covid-19 Employment', 'Transition Period - General', 'Opportunities', 'Exports/Imports', 'Covid-19 Resuming Operations', 'Cashflow', 'Covid-19 Future Expectations', 'Regulation', 'Tariffs', 'Covid-19 Supply Chain/Stock', 'Free Trade Agreements', 'Skills', 'Migration And Immigration', 'Access To Finance', 'Tax', 'Border Arrangements']

# all_tags = [
#     'Access To Finance',
#     'Tax',
# ]

tags_covid = [
    i for i in all_tags if i.lower().startswith('covid') and i.lower() != 'covid-19'
]

tags_general = [i for i in all_tags if i not in tags_covid]

MAX_NB_WORDS = 10000
MAX_SEQUENCE_LENGTH = 500
EMBEDDING_DIM = 100
model_version = 'models_20200907.zip'
model_version = 'models_20201001.zip'
training_data_file = 'training_data_20201001.csv'
training_data_file = 'training_data_20201029.csv'
probability_threshold = 0.4