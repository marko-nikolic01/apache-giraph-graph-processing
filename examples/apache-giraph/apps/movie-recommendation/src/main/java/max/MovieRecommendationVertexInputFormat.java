package mr;

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

public class MovieRecommendationVertexInputFormat extends TextVertexInputFormat<Text, Text, LongWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
        return new MovieRecommendationVertexReader();
    }

    public class MovieRecommendationVertexReader extends TextVertexReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            return line.toString().split("\\s+");
        }

        @Override
        protected Text getId(String[] tokens) throws IOException {
            return new Text(tokens[0]);
        }

        @Override
        protected Text getValue(String[] tokens) throws IOException {
            return new Text("");
        }

        @Override
        protected Iterable<Edge<Text, LongWritable>> getEdges(String[] tokens) throws IOException {
            List<Edge<Text, LongWritable>> edges = new ArrayList<>();

            if (tokens[0].startsWith("U")) {
                for (int i = 1; i < tokens.length; i += 2) {
                    String movie = tokens[i];
                    long rating = Long.parseLong(tokens[i + 1]);
                    edges.add(EdgeFactory.create(new Text(movie), new LongWritable(rating)));
                }
            } else if (tokens[0].startsWith("M")) {
                for (int i = 1; i < tokens.length; i++) {
                    edges.add(EdgeFactory.create(new Text(tokens[i]), new LongWritable(0)));
                }
            }

            return edges;
        }
    }
}
