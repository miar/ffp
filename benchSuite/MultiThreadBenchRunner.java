import java.util.Random;
import java.io.FileReader;
import java.io.Reader;
import java.io.Writer;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;


public class MultiThreadBenchRunner {
   
    private final static int [] THREADS = {1, 8, 16, 24, 32}; 
    private final static int RUNS = 20;
    private final static int WARMUP_RUNS = 5;

    private final static int DATASET_SIZE =  1000000; 
    private final static long DATASET_RANGE = Long.MAX_VALUE;     

    private final static double [] DATASET_INSERT_RATIOS = 
    { 1.00, // 01 - All - Ins
      0.00, // 02 - All - Look
      0.00, // 03 - All - Look
      0.00, // 04 - All - Rem
      
      0.80, // 05 - Mix - Ins = 0.80  Look (to be found) = 0.05 Look (not to be found) = 0.05  Rem = 0.10
      0.60, // 06 - Mix - Ins = 0.60  Look (to be found) = 0.15 Look (not to be found) = 0.15  Rem = 0.10
      0.40, // 07 - Mix - Ins = 0.40  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.10
      0.20, // 08 - Mix - Ins = 0.20  Look (to be found) = 0.35 Look (not to be found) = 0.35  Rem = 0.10
      
      0.50, // 09 - Mix - Ins = 0.50  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.00
      0.00, // 10 - Mix - Ins = 0.00  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.50
      0.50, // 11 - Mix - Ins = 0.50  Look (to be found) = 0.00 Look (not to be found) = 0.00  Rem = 0.50
      0.25, // 12 - Mix - Ins = 0.25  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.25
    };
    
    private final static double [] DATASET_LOOKUP_FOUND_RATIOS = 
    { 0.00, // 01 - All - Ins 
      1.00, // 02 - All - Look 
      0.50, // 03 - All - Look
      0.00, // 04 - All - Rem
      
      0.05, // 05 - Mix - Ins = 0.80  Look (to be found) = 0.05 Look (not to be found) = 0.05  Rem = 0.10
      0.15, // 06 - Mix - Ins = 0.60  Look (to be found) = 0.15 Look (not to be found) = 0.15  Rem = 0.10
      0.25, // 07 - Mix - Ins = 0.40  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.10
      0.35, // 08 - Mix - Ins = 0.20  Look (to be found) = 0.35 Look (not to be found) = 0.35  Rem = 0.10
      
      0.25, // 09 - Mix - Ins = 0.50  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.00
      0.25, // 10 - Mix - Ins = 0.00  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.50
      0.00, // 11 - Mix - Ins = 0.50  Look (to be found) = 0.00 Look (not to be found) = 0.00  Rem = 0.50
      0.25, // 12 - Mix - Ins = 0.25  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.25			  
    };

    private final static double [] DATASET_LOOKUP_NFOUND_RATIOS = 
    { 0.00, // 01 - All - Ins 
      0.00, // 02 - All - Look 
      0.50, // 03 - All - Look 
      0.00, // 04 - All - Rem
      
      0.05, // 05 - Mix - Ins = 0.80  Look (to be found) = 0.05 Look (not to be found) = 0.05  Rem = 0.10
      0.15, // 06 - Mix - Ins = 0.60  Look (to be found) = 0.15 Look (not to be found) = 0.15  Rem = 0.10
      0.25, // 07 - Mix - Ins = 0.40  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.10
      0.35, // 08 - Mix - Ins = 0.20  Look (to be found) = 0.35 Look (not to be found) = 0.35  Rem = 0.10
      
      0.25, // 09 - Mix - Ins = 0.50  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.00
      0.25, // 10 - Mix - Ins = 0.00  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.50
      0.00, // 11 - Mix - Ins = 0.50  Look (to be found) = 0.00 Look (not to be found) = 0.00  Rem = 0.50
      0.25, // 12 - Mix - Ins = 0.25  Look (to be found) = 0.25 Look (not to be found) = 0.25  Rem = 0.25			  
    };
    
    /* CONFIGURATION STUFF - END */
    
    private final static int NR_DATASET_RATIOS = DATASET_INSERT_RATIOS.length;
    private static int LAST_INSERT_I;
    private static int LAST_LOOKUP_FOUND_I;
    private static int LAST_LOOKUP_NFOUND_I;
    private static int MAPS = 5;

    public static void main(String[] args) throws InterruptedException {	

	if (args.length != MAPS + 1) {
	    System.out.println ("Error -> Define the modes for the hash maps");
	    System.exit(0);
	}
	
	int di;	
	Boolean maps [] = new Boolean[MAPS + 1];
	int mi = 0;
	for (String a: args)
	    maps[mi++] = Boolean.valueOf(a);	
	
	
	if (maps[0] == true) {
	    /* generate new datasets */
	    for (di = 0; di < NR_DATASET_RATIOS; di++) {
		LAST_INSERT_I = (int) (DATASET_SIZE * DATASET_INSERT_RATIOS[di]);
		LAST_LOOKUP_FOUND_I = LAST_INSERT_I + 
		    (int) (DATASET_SIZE * DATASET_LOOKUP_FOUND_RATIOS[di]);	    
		
		LAST_LOOKUP_NFOUND_I = LAST_LOOKUP_FOUND_I + 
		    (int) (DATASET_SIZE * DATASET_LOOKUP_NFOUND_RATIOS[di]);	    		
		generateLongRandomDatasets(di);
	    }
	}

	MultiThreadInsertLookupRemoveSpeedup bench = new 
	    MultiThreadInsertLookupRemoveSpeedup (THREADS,
						  RUNS, 
						  WARMUP_RUNS,
						  DATASET_SIZE);
	
	for (di = 0; di < NR_DATASET_RATIOS; di++) {
	    System.out.println("--------------------------------------------------------------------------------------------");
	    System.out.printf("Ratios : Ins = %.2f Look (to be found) = %.2f Look (not to be found) = %.2f Rem = %.2f \n", 
	        DATASET_INSERT_RATIOS[di], DATASET_LOOKUP_FOUND_RATIOS[di],
		DATASET_LOOKUP_NFOUND_RATIOS[di],
	        (1.00 - DATASET_INSERT_RATIOS[di] - DATASET_LOOKUP_FOUND_RATIOS[di] - DATASET_LOOKUP_NFOUND_RATIOS[di]));	    
	    LAST_INSERT_I = 
		(int) (DATASET_SIZE * DATASET_INSERT_RATIOS[di]);
	    LAST_LOOKUP_FOUND_I = 
		LAST_INSERT_I + (int) (DATASET_SIZE * DATASET_LOOKUP_FOUND_RATIOS[di]);
	    
	    LAST_LOOKUP_NFOUND_I = LAST_LOOKUP_FOUND_I + 
		(int) (DATASET_SIZE * DATASET_LOOKUP_NFOUND_RATIOS[di]);	    

    
	    bench.run(di, LAST_INSERT_I, LAST_LOOKUP_FOUND_I,  LAST_LOOKUP_NFOUND_I,
		      maps[1], maps[2], maps[3], maps[4], maps[5]);    			
	}	
    }
    
    public static final void generateLongRandomDatasets(int di){
	/* max random value 2^48 */
	Random randomGenerator = new Random();
	try{
	    FileOutputStream fos = new FileOutputStream(
				   "datasets/dataset_" + di);
	    
	    DataOutputStream file_writer = new DataOutputStream(fos);
	    long OPERATION_RANGE = (long) (DATASET_RANGE / 4);
	    /* items to insert - upper value range limitation */
	    long lower_value = 0; 
	    long upper_value = lower_value + OPERATION_RANGE - 1;
	    for (int ir = 0; 
		 ir < LAST_INSERT_I; ++ir) {
		long rn = lower_value + 
		    (long)(randomGenerator.nextDouble()*(upper_value - lower_value));
		file_writer.writeLong(rn);
	    }	    
	    /* items to lookup - lower value  and upper range limitations */
	    lower_value = upper_value + 1; 
	    upper_value = lower_value + OPERATION_RANGE - 1; 	    
	    for (int ir = LAST_INSERT_I; ir < LAST_LOOKUP_FOUND_I; ++ir) {
		long rn = lower_value + 
		    (long)(randomGenerator.nextDouble()*(upper_value - lower_value));
		file_writer.writeLong(rn); 
	    }
	    lower_value = upper_value + 1; 
	    upper_value = lower_value + OPERATION_RANGE - 1; 	    
	    for (int ir = LAST_LOOKUP_FOUND_I; ir < LAST_LOOKUP_NFOUND_I; ++ir) {
		long rn = lower_value + 
		    (long)(randomGenerator.nextDouble()*(upper_value - lower_value));
		file_writer.writeLong(rn); 
	    }
	    /* items to remove - lower value range limitation */
	    lower_value = upper_value + 1; 
	    upper_value = lower_value + DATASET_RANGE; 
	    for (int ir = LAST_LOOKUP_NFOUND_I; ir < DATASET_SIZE; ++ir) {
		long rn = lower_value + 
		    (long)(randomGenerator.nextDouble()*(upper_value - lower_value));
		file_writer.writeLong(rn);
	    }
	    file_writer.close();		
	} catch (Exception e) {e.printStackTrace();} 
    }
}

