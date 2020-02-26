package SumOfProducts;

	import java.io.IOException;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.DoubleWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

	/**
	 * Break the sales down by store.
	 */
	public class SumOfProducts {

		public final static void main(final String[] args) throws Exception {
			final Configuration conf = new Configuration();

			final Job job = new Job(conf, "SumOfProducts");
			job.setJarByClass(SumOfProducts.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			job.setMapperClass(EpsilonMap.class);
			job.setCombinerClass(EpsilonReduce.class);
			job.setReducerClass(EpsilonReduce.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);
		}

		public static final class EpsilonMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
			private final Text word = new Text();

			public final void map(final LongWritable key, final Text value, final Context context)
					throws IOException, InterruptedException {
				final String line = value.toString();
				
				final String[] data = line.trim().split(",");
				
				// If is the first line, we skip
				if (data[0].equals("Transaction_date")) {
					return;
				}
				try {
					final String product = data[1];
					final double price = Double.parseDouble(data[2]);
					word.set(product);
					context.write(word, new DoubleWritable(price));
				} catch (Exception e) {
					// TODO: handle exception
					System.out.println("Problematic line: ");
					System.out.println(line);
					throw e;
				}
			}
		}

		public static final class EpsilonReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

			public final void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context)
					throws IOException, InterruptedException {
				double sum = 0.0;
				for (final DoubleWritable val : values) {
					sum += val.get();
				}
				context.write(key, new DoubleWritable(sum));
			}
		}
	}
