import numpy as np
from sklearn.cluster import KMeans

def LineEquation(x1, y1, x2, y2):
  m = (y2 - y1) / (x2 - x1)
  c = y1 - (m * x1)
  return -m, 1, -c

def distancePointLine(xi, yi, A, B, C):
  return abs((A * xi + B * yi + C)) / ((A ** 2 + B ** 2) ** 0.5)

def numClusters(maxCluster, ratings):
  wcss = []
  clusters = []
  for cluster in range(1, maxCluster + 1):
    kmeans = KMeans(n_clusters = cluster, init = 'k-means++', random_state = 39)
    kmeans.fit(ratings)
    wcss.append(kmeans.inertia_)
    clusters.append(cluster)
  A, B, C = LineEquation(clusters[0], wcss[0], clusters[maxCluster - 1], wcss[maxCluster - 1])
  clustersNeeded = None
  maxDistance = 0
  for idx in range(0, maxCluster):
    if distancePointLine(clusters[idx], wcss[idx], A, B, C) > maxDistance:
      clustersNeeded = clusters[idx]
      maxDistance = distancePointLine(clusters[idx], wcss[idx], A, B, C)
  return clustersNeeded

def trainModel(cluster, ratings):
  kmeans = KMeans(n_clusters = cluster, init = 'k-means++', random_state = 39)
  kmeans.fit(ratings)
  return kmeans

def partitionDetection(rating, group, centroids):
  numFeatures = len(rating)
  RC_Vector = []
  positive = 0
  negative = 0

  cos_eval = 0
  RC_Mag = 0
  C_Mag = 0

  partition = None

  for idx in range(0, numFeatures):
    RC_Vector.append(rating[idx] - centroids[group][idx])
    if RC_Vector[idx] >= 0:
      positive += 1
    else:
      negative += 1

  for idx in range(0, numFeatures):
    cos_eval += RC_Vector[idx] * centroids[group][idx]
    RC_Mag += RC_Vector[idx] ** 2
    C_Mag += centroids[group][idx] ** 2
  cos_eval = cos_eval / ((RC_Mag ** 0.5) * (C_Mag ** 0.5))

  if cos_eval >= 0:
    if positive == numFeatures:
      partition = 1
    else:
      partition = 2
  else:
    if negative == numFeatures:
      partition = 4
    else:
      partition = 3
  return partition, cos_eval

def minCentroid_maxCentroid(centroids):
  minCentroidDist = 9999999
  maxCentroidDist = 0
  maxDist = (len(centroids[0]) * 25) ** 0.5
  minDist = (len(centroids[0]) * 1) ** 0.5
  for centroid in centroids:
    distCentroid = 0
    for idx in range(0, len(centroid)):
      distCentroid += centroid[idx] ** 2
    distCentroid = distCentroid ** 0.5
    if distCentroid < minCentroidDist:
      minCentroidDist = distCentroid
    if distCentroid > maxCentroidDist:
      maxCentroidDist = distCentroid
  diff = maxCentroidDist - minCentroidDist
  maxDiff = maxDist - minDist
  fraction = diff / maxDiff
  vrange = [0,0]
  vrange[0] = -fraction
  vrange[1] = fraction
  return minCentroidDist, maxCentroidDist, vrange

def rankRange(data, model, centroids):
  centroidRadius = [0 for i in range(0, len(centroids))]
  bestWorst = {}
  for idx in range(0, len(centroids)):
    bestWorst[idx] = []
    for feature in range(0, len(centroids[idx])):
      bestWorst[idx].append([-6, 6])
  results = {}
  for key in data:
    group = model.predict(np.array(data[key]).reshape(1, -1))[0]
    radius = 0
    below = []
    for idx in range(0, len(centroids[group])):
      radius += ((data[key][idx] - centroids[group][idx]) ** 2)
      bestWorst[group][idx][0] = max(bestWorst[group][idx][0], data[key][idx] - centroids[group][idx])
      bestWorst[group][idx][1] = min(bestWorst[group][idx][1], data[key][idx] - centroids[group][idx])
    radius = radius ** 0.5
    centroidRadius[group] = max(centroidRadius[group], radius)
    partition, cos_eval = partitionDetection(data[key], group, centroids)
    results[key] = [group, partition, cos_eval, radius]

  reward_penalty = {}
  minCentroidDist, maxCentroidDist, vrange = minCentroid_maxCentroid(centroids)
  for key in results:
    group = results[key][0]
    partition = results[key][1]
    cos_eval = results[key][2]
    radius = results[key][3]
    ratioFraction = radius / centroidRadius[group]
    curCentroidDist = 0
    for idx in range(0, len(centroids[group])):
      curCentroidDist += centroids[group][idx] ** 2
    curCentroidDist = curCentroidDist ** 0.5
    reference = curCentroidDist - minCentroidDist
    rangeCentreAssigned = ((reference / (maxCentroidDist - minCentroidDist)) * (vrange[1] - vrange[0])) + vrange[0]
    rew_pen = []
    for idx in range(0, len(data[key])):
      diffFeature = data[key][idx] - centroids[group][idx]
      if diffFeature < 0:
        ratioFeature = abs(diffFeature / bestWorst[group][idx][1])
        penalty = (ratioFeature * -0.1) + rangeCentreAssigned
        rew_pen.append(penalty)
      else:
        ratioFeature = abs(diffFeature / bestWorst[group][idx][0])
        reward = (ratioFeature * -0.1) + rangeCentreAssigned
        rew_pen.append(reward)
    reward_penalty[key] = rew_pen
  return reward_penalty

def ML_Profiling(data, max_clusters):
  ratings = []
  for key in data:
    ratings.append(data[key])
  cluster = numClusters(max_clusters, ratings)
  model = trainModel(cluster, ratings)
  centroids = model.cluster_centers_
  centroids = centroids.tolist()
  reward_penalty = rankRange(data, model, centroids)
  return reward_penalty