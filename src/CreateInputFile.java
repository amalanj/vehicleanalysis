import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;


public class CreateInputFile {

	
	//private static final String DATE_FORMAT = "yyyy-MM-dd";
	
	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";

	private static SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
	
	static Random randomGenerator = new Random();
	
	static FileOutputStream fos1 = null;
	static FileOutputStream fos2 = null;
	
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws InterruptedException, IOException {		
		System.out.print("Running.");
		while(true){
			
			Thread.sleep(500);
			createTestData();
			
		}

	}

	private static void createTestData() throws IOException {
		
		fos1 = new FileOutputStream("d:\\amalan\\195_5306.csv",true);
		fos1.write("195_5306".getBytes());
		fos1.write(",".getBytes());
		fos1.write(sdf.format(new Date()).getBytes());
		fos1.write(",".getBytes());
		fos1.write(String.valueOf(randomGenerator.nextInt(100)).getBytes());
		fos1.write(",".getBytes());
		fos1.write(String.valueOf(randomGenerator.nextInt(100)).getBytes());
		fos1.write(",".getBytes());
		fos1.write(String.valueOf(randomGenerator.nextInt(100)).getBytes());
		fos1.write("\n".getBytes());
		System.out.print(".");
		
		fos2 = new FileOutputStream("d:\\amalan\\195_5307.csv",true);
		fos2.write("195_5307".getBytes());
		fos2.write(",".getBytes());
		fos2.write(sdf.format(new Date()).getBytes());
		fos2.write(",".getBytes());
		fos2.write(String.valueOf(randomGenerator.nextInt(100)).getBytes());
		fos2.write(",".getBytes());
		fos2.write(String.valueOf(randomGenerator.nextInt(100)).getBytes());
		fos2.write(",".getBytes());
		fos2.write(String.valueOf(randomGenerator.nextInt(100)).getBytes());
		fos2.write("\n".getBytes());
		System.out.print(".");
		
		fos1.close();
		fos2.close();
		
	}

}
