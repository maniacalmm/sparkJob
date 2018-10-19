import ykzn_spark_jobs.DistinctWord;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        if (args[0].equals("distinct"))
            DistinctWord.compute((String[])Arrays.copyOfRange(args, 1, args.length));
    }
}
