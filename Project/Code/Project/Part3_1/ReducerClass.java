package info7250.bigData.Project.Part3_1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text, Text, Text, IntWritable> {
	
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int counter = 0;
        String movieName = new String();
    	for(Text text : values){
            if(text.charAt(0) == 'A'){
                counter++;
            } else if(text.charAt(0) == 'B'){
                 movieName = (text.toString().substring(1));
            }
        }
        executeJoinLogic(context, counter, movieName);
    }

    public void executeJoinLogic(Context context, int counter, String movieName) throws IOException, InterruptedException {
        String joinType = context.getConfiguration().get("join.type");

        //INNER JOIN OPERATION
        if(joinType.equalsIgnoreCase("inner")){
            if(counter > 0 && movieName.length() > 0){
            	Text movie = new Text();
            	movie.set(movieName);
                context.write(movie, new IntWritable(counter));
            }
        }
    }
}
