package mr;

import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MovieRecommendationVertexOutputFormat extends TextVertexOutputFormat<Text, Text, LongWritable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException {
        return new MovieRecommendationVertexWriter();
    }

    public class MovieRecommendationVertexWriter extends TextVertexWriter {

        @Override
        public void writeVertex(Vertex<Text, Text, LongWritable> vertex) throws IOException, InterruptedException {
            String id = vertex.getId().toString();

            if (id.startsWith("U")) {
                String recommendations = vertex.getValue() != null ? vertex.getValue().toString() : "";
                String outputLine = id + " " + recommendations;
                getRecordWriter().write(null, new Text(outputLine));
            }
        }
    }
}
