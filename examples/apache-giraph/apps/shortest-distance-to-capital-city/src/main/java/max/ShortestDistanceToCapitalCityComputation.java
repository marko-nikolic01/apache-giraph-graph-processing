package sdtcc;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ShortestDistanceToCapitalCityComputation extends BasicComputation<Text, LongWritable, LongWritable, LongWritable> {

    @Override
    public void compute(Vertex<Text, LongWritable, LongWritable> vertex, Iterable<LongWritable> messages) throws IOException {
        long currentDistance = vertex.getValue().get();
        long minDistance = currentDistance;

        for (LongWritable msg : messages) {
            minDistance = Math.min(minDistance, msg.get());
        }

        if (minDistance < currentDistance || getSuperstep() == 0) {
            vertex.setValue(new LongWritable(minDistance));

            for (Edge<Text, LongWritable> edge : vertex.getEdges()) {
                long distanceToNeighbor = minDistance + edge.getValue().get();
                sendMessage(edge.getTargetVertexId(), new LongWritable(distanceToNeighbor));
            }
        }

        vertex.voteToHalt();
    }
}
