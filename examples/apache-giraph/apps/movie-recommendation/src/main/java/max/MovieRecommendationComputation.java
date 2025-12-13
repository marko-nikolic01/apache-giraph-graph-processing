package mr;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

public class MovieRecommendationComputation extends BasicComputation<Text, Text, LongWritable, Text> {

    @Override
    public void compute(Vertex<Text, Text, LongWritable> vertex, Iterable<Text> messages) throws IOException {
        if (getSuperstep() == 0) {
            sendUserRatings(vertex);
        } else if (getSuperstep() == 1) {
            aggregateMovieInfo(vertex, messages);
        } else if (getSuperstep() == 2) {
            computeUserRecommendations(vertex, messages);
        }

        vertex.voteToHalt();
    }

    private void sendUserRatings(Vertex<Text, Text, LongWritable> vertex) {
        if (!vertex.getId().toString().startsWith("U")) {
            return;
        }

        for (Edge<Text, LongWritable> fromEdge : vertex.getEdges()) {
            String message = vertex.getId() + "," + fromEdge.getTargetVertexId() + "," + fromEdge.getValue().get();
            sendMessageToAllEdges(vertex, new Text(message));
        }
    }

    private void aggregateMovieInfo(Vertex<Text, Text, LongWritable> vertex, Iterable<Text> messages) {
        if (!vertex.getId().toString().startsWith("M")) {
            return;
        }

        Map<String, int[]> ratings = new HashMap<>();

        for (Text message : messages) {
            String[] parts = message.toString().split(",");
            String userId = parts[0];
            String movieId = parts[1];
            int rating = Integer.parseInt(parts[2]);

            ratings.putIfAbsent(movieId, new int[]{0, 0});
            ratings.get(movieId)[0] += rating;
            ratings.get(movieId)[1] += 1;
        }

        for (Map.Entry<String, int[]> rating : ratings.entrySet()) {
            String movieId = rating.getKey();
            int sum = rating.getValue()[0];
            int count = rating.getValue()[1];
            sendMessageToAllEdges(vertex, new Text(movieId + "," + sum + "," + count));
        }
    }

    private void computeUserRecommendations(Vertex<Text, Text, LongWritable> vertex, Iterable<Text> messages) {
        if (!vertex.getId().toString().startsWith("U")) {
            return;
        }

        Set<String> seen = new HashSet<>();

        for (Edge<Text, LongWritable> edge : vertex.getEdges()) {
            seen.add(edge.getTargetVertexId().toString());
        }

        Map<String, int[]> ratings = new HashMap<>();

        for (Text message : messages) {
            String[] parts = message.toString().split(",");
            String movieId = parts[0];
            int sum = Integer.parseInt(parts[1]);
            int count = Integer.parseInt(parts[2]);

            ratings.putIfAbsent(movieId, new int[]{0, 0});
            ratings.get(movieId)[0] += sum;
            ratings.get(movieId)[1] += count;
        }

        Map<String, Double> averageRatings = new HashMap<>();

        for (Map.Entry<String, int[]> rating : ratings.entrySet()) {
            if (!seen.contains(rating.getKey())) {
                averageRatings.put(rating.getKey(), (double) rating.getValue()[0] / rating.getValue()[1]);
            }
        }

        List<Map.Entry<String, Double>> sortedMovies = new ArrayList<>(averageRatings.entrySet());
        sortedMovies.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

        List<String> top3 = new ArrayList<>();
        
        for (int i = 0; i < Math.min(3, sortedMovies.size()); i++) {
            top3.add(sortedMovies.get(i).getKey());
        }

        vertex.setValue(new Text(String.join(" ", top3)));
    }
}
