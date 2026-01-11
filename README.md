# Apache Giraph - Graph processing

This repository contains Dockerized examples of Apache Giraph applications, including Max Value, Shortest Distance to Capital City, and Movie Recommendation, demonstrating distributed vertex-centric graph processing.


<p align="center">
<img width="205" height="246" alt="images" src="https://github.com/user-attachments/assets/3812a70e-43b2-46e7-b399-97742ddda59c" />
</p>

## How to run

### Run container

```
cd examples && \
docker compose up --build
```

### Use container interactively

```
docker exec -it apache-giraph bash
```

### Run example apps

1. Max value

```
cd /opt/apps/max-value && \
OUT="test-$(date +%Y%m%d-%H%M%S)" && \
hadoop jar target/max-value-1.0.jar \
  org.apache.giraph.GiraphRunner \
  mv.MaxValueComputation \
  -vif mv.MaxValueVertexInputFormat \
  -vof mv.MaxValueVertexOutputFormat \
  -vip /data/input/max-value-graph.txt \
  -op /data/output/$OUT \
  -w 1 \
  -ca giraph.SplitMasterWorker=false
```

2. Shortest distance to capital city

```
cd /opt/apps/shortest-distance-to-capital-city && \
OUT="test-$(date +%Y%m%d-%H%M%S)" && \
hadoop jar target/shortest-distance-to-capital-city-1.0.jar \
  org.apache.giraph.GiraphRunner \
  sdtcc.ShortestDistanceToCapitalCityComputation \
  -vif sdtcc.ShortestDistanceToCapitalCityVertexInputFormat \
  -vof sdtcc.ShortestDistanceToCapitalCityVertexOutputFormat \
  -vip /data/input/shortest-distance-to-capital-city-graph.txt \
  -op /data/output/$OUT \
  -w 1 \
  -ca giraph.SplitMasterWorker=false
```

3. Movie recommendation

```
cd /opt/apps/movie-recommendation && \
OUT="test-$(date +%Y%m%d-%H%M%S)" && \
hadoop jar target/movie-recommendation-1.0.jar \
  org.apache.giraph.GiraphRunner \
  mr.MovieRecommendationComputation \
  -vif mr.MovieRecommendationVertexInputFormat \
  -vof mr.MovieRecommendationVertexOutputFormat \
  -vip /data/input/movie-recommendation-graph.txt \
  -op /data/output/$OUT \
  -w 1 \
  -ca giraph.SplitMasterWorker=false
```

### Check the output

```
cd examples/apache-giraph/output
```
