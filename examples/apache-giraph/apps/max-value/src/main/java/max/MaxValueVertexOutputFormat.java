package mv;

import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MaxValueVertexOutputFormat extends TextVertexOutputFormat<LongWritable, LongWritable, NullWritable> {

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new MaxValueVertexWriter();
    }

    public class MaxValueVertexWriter extends TextVertexWriter {
        @Override
        public void writeVertex(Vertex<LongWritable, LongWritable, NullWritable> vertex) throws IOException, InterruptedException {
            getRecordWriter().write(new Text(vertex.getId().toString()), new Text(vertex.getValue().toString()));
        }
    }
}
