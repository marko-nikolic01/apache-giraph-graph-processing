package mv;

import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MaxValueVertexInputFormat extends TextVertexInputFormat<LongWritable, LongWritable, NullWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
        return new MaxValueVertexReader();
    }

    public class MaxValueVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            return line.toString().split("\\s+");
        }

        @Override
        protected LongWritable getId(String[] tokens) throws IOException {
            return new LongWritable(Long.parseLong(tokens[0]));
        }

        @Override
        protected LongWritable getValue(String[] tokens) throws IOException {
            return new LongWritable(Long.parseLong(tokens[1]));
        }

        @Override
        protected Iterable<Edge<LongWritable, NullWritable>> getEdges(String[] tokens) throws IOException {
            List<Edge<LongWritable, NullWritable>> edges = new ArrayList<>();

            for (int i = 2; i < tokens.length; i++) {
                edges.add(EdgeFactory.create(new LongWritable(Long.parseLong(tokens[i])), NullWritable.get()));
            }
            
            return edges;
        }
    }
}
