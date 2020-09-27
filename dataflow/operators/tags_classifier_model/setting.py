all_tags = [
    'Covid-19',
    'Covid-19 Feedback On Hmg Support',
    'Covid-19 Business Disruption',
    'Covid-19 Employment',
    'Covid-19 Request For Hmg Support',
    'Covid-19 Opportunity',
    'Covid-19 Exports',
    'Investment',
    'Covid-19 Cash Flow',
    'Eu Exit',
    'Covid-19 Future Expectations',
    'Covid-19 Offers Of Support',
    'Covid-19 Supply Chain/Stock',
    'Tariffs',
    'Export',
    'Regulation',
    'Covid-19 Investment',
    'Movement Of People',
    'Skills',
    'Covid-19 Resuming Operations',
    'Access To Finance',
    'Free Trade Agreements',
    'Tax',
]

tags_general = [
    i for i in all_tags if not i.lower().startswith('covid') or i.lower() == 'covid-19'
]
tags_covid = [
    i for i in all_tags if i.lower().startswith('covid') and i.lower() != 'covid-19'
]

MAX_NB_WORDS = 10000
MAX_SEQUENCE_LENGTH = 500
EMBEDDING_DIM = 100
model_version = 'models_20200907'
