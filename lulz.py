import json 

with open("subreddits_purgar.txt") as f:
    subreddits = []
    for line in f:
        subreddits.append(line.strip())
    
    json_to_dump = {"subreddit":subreddits}
    with open("filter.json", "w") as g:
        json.dump(json_to_dump, g)