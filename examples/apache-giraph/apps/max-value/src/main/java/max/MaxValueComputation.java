package mv;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.edge.Edge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class MaxValueComputation extends BasicComputation<LongWritable, LongWritable, NullWritable, LongWritable> {

    @Override
    public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex, Iterable<LongWritable> messages) throws IOException {
        long currentMax = vertex.getValue().get();

        for (LongWritable msg : messages) {
            currentMax = Math.max(currentMax, msg.get());
        }

        if (currentMax > vertex.getValue().get() || getSuperstep() == 0) {
            vertex.setValue(new LongWritable(currentMax));
            for (Edge<LongWritable, NullWritable> edge : vertex.getEdges()) {
                sendMessage(edge.getTargetVertexId(), new LongWritable(currentMax));
            }
        }

        vertex.voteToHalt();
    }
}
