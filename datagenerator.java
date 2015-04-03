package submit;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class datagenerator {
	public static void main(String[] args) throws IOException{
		int number = 0;
		int i = -1;
		FileWriter f = new FileWriter("ten.txt");
		Random random = new Random(System.currentTimeMillis());
		while((++i)<1000000){
			number = (int)(random.nextInt(10));
			f.write(String.valueOf(number)+"\n");
		}
		f.flush();
		f.close();
		f = new FileWriter("hundred.txt");
		random = new Random(System.currentTimeMillis());
		while((++i)<1000000){
			number = (int)(random.nextInt(100));
			f.write(String.valueOf(number)+"\n");
		}
		f.flush();
		f.close();
		f = new FileWriter("thousand.txt");
		random = new Random(System.currentTimeMillis());
		while((++i)<1000000){
			number = (int)(random.nextInt(1000));
			f.write(String.valueOf(number)+"\n");
		}
		f.flush();
		f.close();
		f = new FileWriter("five_th.txt");
		random = new Random(System.currentTimeMillis());
		while((++i)<1000000){
			number = (int)(random.nextInt(5000));
			f.write(String.valueOf(number)+"\n");
		}
		f.flush();
		f.close();
		f = new FileWriter("ten_th.txt");
		random = new Random(System.currentTimeMillis());
		while((++i)<1000000){
			number = (int)(random.nextInt(10000));
			f.write(String.valueOf(number)+"\n");
		}
		f.flush();
		f.close();
		f = new FileWriter("twenty_th.txt");
		random = new Random(System.currentTimeMillis());
		while((++i)<1000000){
			number = (int)(random.nextInt(20000));
			f.write(String.valueOf(number)+"\n");
		}
		f.flush();
		f.close();
	}

}
