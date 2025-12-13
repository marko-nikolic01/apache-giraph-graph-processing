package sdtcc;

import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class ShortestDistanceToCapitalCityVertexOutputFormat extends TextVertexOutputFormat<Text, LongWritable, LongWritable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException {
        return new ShortestDistanceToCapitalCityVertexWriter();
    }

    public class ShortestDistanceToCapitalCityVertexWriter extends TextVertexWriter {

        @Override
        public void writeVertex(Vertex<Text, LongWritable, LongWritable> vertex) throws IOException, InterruptedException {
            String outputLine = vertex.getId().toString() + " " + vertex.getValue().get();
            getRecordWriter().write(null, new Text(outputLine));
        }
    }
}
