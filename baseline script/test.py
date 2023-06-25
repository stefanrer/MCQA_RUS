import json

with open("data.json", "r") as f:
    res = json.load(f)
    pos = 0
    neg = 0
    neutral = 0

    for key in res.keys():
        for k in res[key].values():
            for i in k:
                if i['adj_type'] is None:
                    neutral += 1
                if i['adj_type'] == 'pos':
                    pos += 1
                if i['adj_type'] == 'neg':
                    neg += 1

    print(pos, neg, neutral)
    # print(res)

    print(res["1.0|3"])