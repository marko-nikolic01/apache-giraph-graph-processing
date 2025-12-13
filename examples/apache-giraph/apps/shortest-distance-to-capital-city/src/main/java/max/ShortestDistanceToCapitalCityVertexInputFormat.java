package sdtcc;

import org.apache.giraph.io.formats.TextVertexInputFormat;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShortestDistanceToCapitalCityVertexInputFormat extends TextVertexInputFormat<Text, LongWritable, LongWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
        return new ShortestDistanceToCapitalCityVertexReader();
    }

    public class ShortestDistanceToCapitalCityVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            return line.toString().split("\\s+");
        }

        @Override
        protected Text getId(String[] tokens) throws IOException {
            return new Text(tokens[0]);
        }

        @Override
        protected LongWritable getValue(String[] tokens) throws IOException {
            return new LongWritable(Long.parseLong(tokens[1]));
        }

        @Override
        protected Iterable<Edge<Text, LongWritable>> getEdges(String[] tokens) throws IOException {
            List<Edge<Text, LongWritable>> edges = new ArrayList<>();

            for (int i = 2; i < tokens.length; i += 2) {
                edges.add(EdgeFactory.create(new Text(tokens[i]), new LongWritable(Long.parseLong(tokens[i + 1]))));
            }

            return edges;
        }
    }
}
