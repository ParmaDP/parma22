package parmanix;

// 

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MapperWrapper extends Mapper<Object, Text, Text, DoubleWritable> {
//  static boolean JIKES = false;
	public static String getInputGroup(String line) {
		// TODO: Analyze the line and assign an actual group (Data Provider's job)
		return "G";
	}

	private static void enforceRange(Double minValue, Double maxValue, ArrayList<String[]> untrustedMapperResult)
			throws Exception {
		for (int i = 0; i < untrustedMapperResult.size(); i++) {
			String[] pair = untrustedMapperResult.get(i);
			String key = pair[0];
			Double value = Double.parseDouble(pair[1]);

			// Enforce the range
			Double minValDouble = minValue.doubleValue();
			Double maxValDouble = maxValue.doubleValue();

			if (minValDouble > maxValDouble) {
				throw new Exception("minValue needs to be smaller than maxValue");
			}
			Double mid = minValDouble + (minValDouble + maxValDouble) / 2.0;
			if (value < minValDouble || value > maxValDouble) {
				pair[1] = mid.toString();
//				untrustedMapperResult.set(i, Pair.create(key, mid.toString()));
			}
		}
	}

	public void map(Object someUnkownKey, Text currentLine, Context context) throws IOException, InterruptedException {
		
		Configuration currentConfiguration = context.getConfiguration();
		// Custom map depending on dataset
		String dataset = currentConfiguration.get("dataset");
		ArrayList<String[]> kvPairs = new ArrayList<String[]>();
		// TODO: Here add custom dataset mappers
		if ( dataset.compareToIgnoreCase("Bigshop") == 0 ) {
			// TODO: Check p.7 that says each input should be mapped to a list of K,V pair
			kvPairs = getBigshopProductCountPair(currentLine.toString());	
			
		} else if (dataset.compareToIgnoreCase("EpsilonFull") == 0) {
			// TODO: Choose which columns we return to the untrusted mapper
			// Right now, we will provide full access
			kvPairs = getEpsilonFullPair(currentLine.toString());
		} else if (dataset.compareToIgnoreCase("EpsilonAnonymized") == 0) {
			// TODO: Choose which columns we return to the untrusted mapper
			// Right now, we will provide full access
			kvPairs = getEpsilonAnonymizedPairByProduct(currentLine.toString());
		} else {
			// TODO: Deal correctly with this
			System.out.println("Configuration for dataset not supported: " + dataset);
		}
