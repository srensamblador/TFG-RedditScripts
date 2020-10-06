import praw
import pickle

with open("authors.pickle", "rb") as f:
    authors = pickle.load(f)

reddit = praw.Reddit(
    client_id ="ZV1B8BM_v2qYLw",
    client_secret ="CNF1jDWyqCfKBMi139DF-d0aDf0",
    user_agent = "UNIOVI"
)

users = [praw.models.Redditor(reddit, name=author) for author in authors]

for user in users:
    try:
        print(user.name, user.comment_karma, user.link_karma, user.created_utc)
    except:
        print("Error")