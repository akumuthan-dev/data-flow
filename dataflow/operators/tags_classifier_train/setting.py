tags = ['Covid-19', 'Covid-19 Feedback On Hmg Support',
        'Covid-19 Business Disruption', 'Covid-19 General', 'Covid-19 Employment',
        'Covid-19 Request For Hmg Support', 'Covid-19 Opportunity', 'Covid-19 Exports',
        'Covid-19 Offers Of Support', 'Covid-19 Cash Flow', 'Covid-19 Future Expectations',
        'Covid-19 Supply Chain/Stock', 'Covid-19 Resuming Operations', 'Covid-19 Investment', 'Investment',
        'Eu Exit', 'Tariffs', 'Export', 'Regulation', 'Access To Finance', 'Covid-19 Dit Delivering For Hmg', 'Movement Of People', 'Skills']

tags_general = [i for i in tags if not i.lower().startswith('covid')  or i.lower() == 'covid-19']
tags_covid = [i for i in tags if i.lower().startswith('covid') and i.lower() != 'covid-19']

#
# tags_general = ['Covid-19']
# tags_general = []
# tags_covid = ['Covid-19 Feedback On Hmg Support']
#
# tags_covid = [ 'Covid-19 Employment',   'Covid-19 Cash Flow',  'Covid-19 Supply Chain/Stock', ## 'Covid-19 Exports/Imports',
#          'Covid-19 Feedback On Hmg Support',  'Covid-19 Request For Hmg Support',
#          'Covid-19 Offers Of Support', 'Covid-19 Business Disruption',
#          'Covid-19 Future Expectations', 'Covid-19 Opportunity', 'Covid-19 Resuming Operations', 'Covid-19 Investment']
#
# tags_general = ['COVID-19', 'Investment', 'Movement of people', 'Movement of goods',
#  'Tariffs', 'Regulation', 'Stock/Supply chain', 'Access to finance', '(Post) transition Period',
#  'Border arrangements', 'Tax','Free Trade Agreements']

#
# tags_general_dev = ['COVID-19']
# tags_covid_dev = tags_covid.copy()


# tags = tags_covid+tags_general

run_mode = 'prod'

#
# ['Covid-19 Feedback On Hmg Support', 'Covid-19 Business Disruption', 'Covid-19 General', 'Covid-19 Employment',
#  'Covid-19 Request For Hmg Support', 'Covid-19 Opportunity', 'Covid-19 Exports',
#  'Covid-19 Offers Of Support', 'Covid-19 Cash Flow', 'Covid-19 Future Expectations', 'Covid-19 Supply Chain/Stock',
#  'Covid-19 Resuming Operations', 'Covid-19 Investment',
#  'Investment', 'Eu Exit', 'Tariffs', 'Export', 'Regulation', 'Access To Finance',
#  'Covid-19 Dit Delivering For Hmg', 'Movement Of People', 'Skills']


MAX_NB_WORDS = 10000
# Max number of words in each sentence.
MAX_SEQUENCE_LENGTH = 500
EMBEDDING_DIM = 100
