from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
import splink.duckdb.duckdb_comparison_template_library as ctl

import difflib
import pandas as pd

#from fuzzywuzzy import fuzz
#from fuzzywuzzy import process
from rapidfuzz import fuzz
from rapidfuzz import process

def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=2):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with boths keys and matches
    """
    s = df_2[key2].tolist()
    
    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))    
    df_1['matches'] = m
    
    m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches'] = m2
    
    return df_1

GOOGLE_FILE = 'preprocessed/google_dataset.csv'
FACEBOOK_FILE = 'preprocessed/facebook_dataset.csv'
WEBSITE_FILE = 'inputs/website_dataset.csv'

google_df = pd.read_csv(GOOGLE_FILE,dtype='unicode',escapechar='\\')
facebook_df = pd.read_csv(FACEBOOK_FILE,dtype='unicode')
website_df = pd.read_csv(WEBSITE_FILE,dtype='unicode',sep=';')

google_df = google_df.applymap(lambda s: s.lower() if type(s) == str else s)
facebook_df = facebook_df.applymap(lambda s: s.lower() if type(s) == str else s)
website_df = website_df.applymap(lambda s: s.lower() if type(s) == str else s)

google_df.columns = 'google_' + google_df.columns.values
facebook_df.columns = 'fb_' + facebook_df.columns.values
website_df.columns = 'web_' + website_df.columns.values

us_google_df = google_df.loc[google_df['google_country_code'] == 'us']
print(us_google_df)
us_facebook_df = facebook_df.loc[facebook_df['fb_country_code'] == 'us']
print(us_facebook_df)

#fuzzy_merge(us_facebook_df, us_google_df, 'fb_name', 'google_name', 80)

goog_fb_df = google_df.merge(facebook_df, left_on='google_name', right_on='fb_name',how='left')

goog_fb_web_df = goog_fb_df.merge(website_df, left_on='google_name', right_on='web_legal_name',how='left')

google_df.drop_duplicates(subset='google_name', inplace=True)
facebook_df.drop_duplicates(subset='fb_name', inplace=True)
website_df.drop_duplicates(subset='web_legal_name', inplace=True)

goog_fb_df = google_df.merge(facebook_df, left_on='google_name', right_on='fb_name',how='inner')

goog_fb_web_df = goog_fb_df.merge(website_df, left_on='google_name', right_on='web_legal_name',how='inner')

print(goog_fb_df[['google_name', 'fb_name', 'google_phone', 'fb_phone']])
print(goog_fb_web_df[['google_name', 'web_legal_name']])

phone_match_goog_fb = len(goog_fb_df.query('google_phone == fb_phone'))
domain_match_goog_fb = len(goog_fb_df.query('google_domain == fb_domain'))
country_match_goog_fb = len(goog_fb_df.query('google_country_name == fb_country_name'))
country_code_match_goog_fb = len(goog_fb_df.query('google_country_code == fb_country_code'))


goog_fb_web_df['fb_phone'] = goog_fb_web_df['fb_phone'].str[1:]
goog_fb_web_df['google_phone'] = goog_fb_web_df['google_phone'].str[1:]

phone_match_goog_fb_web = len(goog_fb_web_df.query('google_phone == fb_phone & fb_phone == web_phone'))
domain_match_goog_fb_web = len(goog_fb_web_df.query('google_domain == fb_domain & fb_domain == web_root_domain'))
country_code_match_goog_fb_web = len(goog_fb_web_df.query('google_country_code == fb_country_code & fb_country_code == web_tld'))


# Get the table headers
go_headers = google_df.columns
fb_headers = facebook_df.columns
web_headers = website_df.columns
# Print the table headers
print(go_headers, fb_headers, web_headers)

print(phone_match_goog_fb)
print(domain_match_goog_fb)
print(country_match_goog_fb)
print(country_code_match_goog_fb)

print(country_code_match_goog_fb_web)
print(phone_match_goog_fb_web)
print(domain_match_goog_fb_web)

goog_fb_clean = goog_fb_df.query('google_phone == fb_phone & google_country_code == fb_country_code')

goog_fb_clean.columns = goog_fb_clean.columns.str.replace('google_phone', 'phone')
goog_fb_clean.columns = goog_fb_clean.columns.str.replace('google_name', 'name')
goog_fb_clean.columns = goog_fb_clean.columns.str.replace('google_domain', 'domain')
goog_fb_clean.columns = goog_fb_clean.columns.str.replace('google_country_code', 'country_code')

goog_fb_clean.to_csv('results/google_facebook.csv', columns=['name', 'country_code', 'phone', 'domain'], index=False)

goog_fb_web_clean = goog_fb_web_df.query('google_phone == fb_phone & google_country_code == fb_country_code & fb_phone == web_phone & fb_country_code == web_tld')

goog_fb_web_clean.columns = goog_fb_web_clean.columns.str.replace('google_phone', 'phone')
goog_fb_web_clean.columns = goog_fb_web_clean.columns.str.replace('google_name', 'name')
goog_fb_web_clean.columns = goog_fb_web_clean.columns.str.replace('google_domain', 'domain')
goog_fb_web_clean.columns = goog_fb_web_clean.columns.str.replace('google_country_code', 'country_code')

goog_fb_web_clean.to_csv('results/google_facebook_web.csv', columns=['name', 'country_code', 'phone', 'domain'], index=False)
