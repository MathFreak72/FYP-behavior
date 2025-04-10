def Mapper(filepath):
    aggregationMap = {}
    with open(filepath, "r") as file:
        for line in file:
            init_ratings = line.split(",")
            ID = init_ratings[0]
            ratings = [int(init_ratings[idx]) for idx in range(1,len(init_ratings))] 
            if ID not in aggregationMap:
                emptyFeatures = [0 for idx in range(0, len(ratings))]
                aggregationMap[ID] = {"numRatings" : 0, "aggFeatures" : emptyFeatures}
            aggregationMap[ID]["numRatings"] += 1
            for idx in range(0, len(ratings)):
                aggregationMap[ID]["aggFeatures"][idx] += int(ratings[idx])
    return aggregationMap

def Reducer(aggregationMap):
    reducedMap = {}
    for ID in aggregationMap:
        totalRatings = aggregationMap[ID]["numRatings"]
        aggFeatures = aggregationMap[ID]["aggFeatures"]
        reducedFeatures = [feature / totalRatings for feature in aggFeatures]
        reducedMap[ID] = reducedFeatures
    return reducedMap