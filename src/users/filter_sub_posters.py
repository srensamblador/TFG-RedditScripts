from psaw import PushshiftAPI
from datetime import datetime as dt

def main():
    twins = []
    with open("twins.csv") as f:
        next(f)
        for line in f:
            twins.append(line.split(";")[1])
    
    api = PushshiftAPI()
    before_date = dt(year=2019, month=12, day=31, hour=23, minute=59, second=59)
    prev = 0
    for i in range(len(twins), step=100):
        api.search_submissions(author=twins[0:i], aggs="subreddit", before = int(before_date.timestamp()))
        #https://api.pushshift.io/reddit/search/submission/?author=watchful1,kaisserds&subreddit=competitiveoverwatch&aggs=author,subreddit

        prev = i

if __name__ == "__main__":
    main()