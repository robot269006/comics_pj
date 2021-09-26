import requests
from IPython.display import display
import pandas as pd
import time
from random import randint

query = '''
query ($page: Int, $perPage: Int, $averageScore_greater: Int) {
    Page (page: $page, perPage: $perPage) {
        media(type: MANGA, averageScore_greater: $averageScore_greater, sort: SCORE_DESC) {
            id
            title {
                native
                romaji
            }
            startDate {
                year
            }
            chapters
            volumes
            status
            countryOfOrigin
            isAdult
            genres
            averageScore
            meanScore
            popularity
            favourites
            stats {
                scoreDistribution {
                    score
                    amount
                }
                statusDistribution {
                    status
                    amount
                }
            }
            rankings {
                type
                rank
            }
            description
            coverImage {
                large
            }
            tags {
                id
                name
                category
                rank
            }
            reviews {
                edges {
                    node {
                        id
                        media {
                            id
                            title {
                                native
                                romaji
                            }
                        }
                        user {
                            id
                            name
                        }
                        rating
                        summary
                        score
                        ratingAmount
                        body
                    }
                }
            }
        }
    }
}
'''

# review_query = '''
# query ($page: Int, $perPage: Int, $averageScore_greater: Int) {
#     Page (page: $page, perPage: $perPage) {
#         media(type: MANGA, averageScore_greater: $averageScore_greater) {
#         }
#     }
# }
# '''

# ================================ func to handle "reviews" table ================================

def process_reviews(reviews, review_array):
    nodes = reviews['edges']
    for node in nodes:
        dict_r = {}
        n = node['node']
        dict_r['review_id'] = n['id']
        dict_r['title_id'] = n['media']['id']
        dict_r['title_native'] = n['media']['title']['native']
        dict_r['title_romaji'] = n['media']['title']['romaji']
        dict_r['user_id'] = n['user']['id']
        dict_r['user_name'] = n['user']['name']
        dict_r['score'] = n['score'] # The score that the reviewer gave to the title
        dict_r['rating'] = n['rating'] # how many users liked the review (ex: if 10 out of 148 users liked this review, this field is 10)
        dict_r['ratingCount'] = n['ratingAmount'] # total number of users who evaluated this review (ex: if 10 out of 148 users liked this review, this field is 148)
        dict_r['text_summary'] = n['summary']
        dict_r['text_body'] = n['body']
        review_array.append(dict_r)

# ================================ func to handle "tags" table ================================

def process_tags(tags, title_id, title, tags_array):
    for tag in tags:
        dict_tags = {}
        dict_tags['tag_id'] = tag['id']
        dict_tags['tag_name'] = tag['name']
        dict_tags['tag_category'] = tag['category']
        # how tag rank is decided -> https://anilist.co/forum/thread/1991
        dict_tags['tag_rank'] = tag['rank'] # The relevance ranking of the tag out of the 100 for this media
        dict_tags['title_id'] = title_id
        dict_tags['title_native'] = title['native']
        dict_tags['title_romaji'] = title['romaji']
        tags_array.append(dict_tags)

# ================================ "titles" table ================================

def process_title(media, titles_array, review_array, tags_array):
    for t in media:
        dict_t = {}
        dict_t['title_id'] = t['id']
        dict_t['title_native'] = t['title']['native']
        dict_t['title_romaji'] = t['title']['romaji']
        dict_t['start_year'] = t['startDate']['year']
        dict_t['chapters'] = t['chapters'] # The amount of chapters the manga has when complete
        dict_t['volume'] = t['volumes'] # The amount of volumes the manga has when complete
        dict_t['publishing_status'] = t['status'] # The current releasing status of the media
        dict_t['country'] = t['countryOfOrigin']
        dict_t['adult'] = t['isAdult'] # If the media is intended only for 18+ adult audiences
        dict_t['genres'] = t['genres']
        dict_t['average_score'] = t['averageScore'] # A weighted average score of all the user's scores of the media
        dict_t['mean_score'] = t['meanScore'] # Mean score of all the user's scores of the media
        dict_t['popularity'] = t['popularity'] # The number of users with the media on their list
        dict_t['favorites'] = t['favourites'] # The amount of user's who have favourited the media
        for s in t['stats']['scoreDistribution']:
            score = s['score']
            dict_t['score_%s'%score] = s['amount']
        for s in t['stats']['statusDistribution']:
            status = s['status']
            dict_t['count_%s'%status] = s['amount']
        for r in t['rankings']:
            rank_type = r['type']
            dict_t['ranking_%s'%rank_type] = r['rank'] # The ranking of the media in a particular time span and format compared to other media
        dict_t['synopsis'] = t['description'] # Synopsis
        dict_t['cover_image_url'] = t['coverImage']
        process_reviews(t['reviews'], review_array)
        process_tags(t['tags'], t['id'], t['title'], tags_array)
        titles_array.append(dict_t)

# ================================ running in loop ================================

TITLES = []
REVIEWS = []
TAGS = []

for page in range(1,101): #21 initially -> 101 for 5000 titles
    print("page: ", page)
    sleep_sec = randint(1,20)
    time.sleep(sleep_sec)
    print("sleep sec: ", sleep_sec)
    variables = {
        'page': page, # 1-20
        'perPage': 50, # max is 50
        'averageScore_greater': 20 # 74 (should take about 750 titles)
    }
    url = 'https://graphql.anilist.co'
    response = requests.post(url, json={'query': query, 'variables': variables})
    json_obj = response.json()
    media = json_obj['data']['Page']['media']
    process_title(media, TITLES, REVIEWS, TAGS)


# ================================ putting everything in csv ================================

cols_titles = TITLES[0].keys()
print(cols_titles)
df_t = pd.DataFrame(TITLES, columns=cols_titles)
print("============================== titles table ==============================")
print("unique titles in titles table: ", len(df_t['title_native'].unique()))
print("len of titles table: ", len(df_t))
display(df_t.head())
df_t.to_csv('./dataset/titles_5000.csv', index=False)

cols_review = REVIEWS[0].keys()
print(cols_review)
df_r = pd.DataFrame(REVIEWS, columns=cols_review)
print("============================== reviews table ==============================")
print("unique titles in reviews table: ", len(df_r['title_native'].unique()))
print("len of reviews table: ", len(df_r))
display(df_r.head())
df_r.to_csv('./dataset/reviews_5000.csv')

cols_tags = TAGS[0].keys()
print(cols_tags)
df_tag = pd.DataFrame(TAGS, columns=cols_tags)
print("============================== tags table ==============================")
print("unique titles in tags table: ", len(df_tag['title_native'].unique()))
print("len of tags table: ", len(df_tag))
display(df_tag.head())
df_tag.to_csv('./dataset/tags_5000.csv')
