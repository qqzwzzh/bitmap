package third;

import java.awt.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Scanner;
import java.util.TreeMap;

public class sample{
	private final static int SIZE = 1300000;
	//�����������ֵ������ֵ��ŵĶ�Ӧ��ϵ��hash����ȱ��һ����������Ӧ��
	private static Hashtable<String,Integer> hash1 = new Hashtable<String,Integer>();
	private static Hashtable<String,Integer> hash2 = new Hashtable<String,Integer>();
	private static Hashtable<String,Integer> hash3 = new Hashtable<String,Integer>();
    private static int amountOfatr1 = 0;
    private static int amountOfatr2 = 0;
    private static int amountOfatr3 = 0;
    private static List atr1 = new List(),atr2=new List(),atr3=new List();
	public static void main(String[] args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, IOException{
		System.out.println(System.currentTimeMillis());
		String url="jdbc:mysql://121.194.104.118/france_newpaper";
	    String user="root";
	    String pwd="62288848";
	    //������������һ��Ҳ��дΪ��
		// Class.forName("com.mysql.jdbc.Driver");
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		//������MySQL������
		Connection conn = DriverManager.getConnection(url,user, pwd);
		//ִ��SQL���
		Statement stmt = conn.createStatement();//��������������ִ��sql����
		String sql = "select distinct(journal_id) from MC_out_preprocess group by journal_id";
		ResultSet rs = stmt.executeQuery(sql);
		while(rs.next()){
			hash1.put(rs.getString(1),amountOfatr1++);
			atr1.add(rs.getString(1));
		}
		sql = "select distinct(newstand_id) from MC_out_preprocess group by newstand_id";
		rs = stmt.executeQuery(sql);
		while(rs.next()){
			hash2.put(rs.getString(1),amountOfatr2++);
			atr2.add(rs.getString(1));
		}
		sql = "select distinct(date) from MC_out_preprocess group by date";
		rs = stmt.executeQuery(sql);
		while(rs.next()){
			hash3.put(rs.getString(1),amountOfatr3++);
			atr3.add(rs.getString(1));
		}
		rs.close();
		stmt.close();
		conn.close();
		System.out.println("��ʼ�������ļ�");
		System.out.println(System.currentTimeMillis());
		FileReader file = new FileReader("D:\\ѧУ\\ѧϰ\\JAVAʵ������ض���\\������������\\CUFE\\new data\\λͼ��ȡ�ļ�0\\MC_out_preprocess.txt");
		BufferedReader br = new BufferedReader(file);
		String str = br.readLine();
		str = br.readLine();
		//���ڼ�¼������һ�У����⣺��û���������Ը������Ը������ɵ�ʹ���ֽ�����
		int byte_index=0;
		byte[] by1 = new byte[SIZE];
		byte[] by2 = new byte[2*SIZE];
		byte[] by3 = new byte[SIZE];
		int t=0;
		while(str!=null){
			str = str.replace("\"", "");
			String[] split = str.split(",");
			if(hash1.containsKey(split[1])){
				t = hash1.get(split[1]);
				//by1[byte_index] = (byte) (t/256);
				by1[byte_index/2] = (byte) t;
			}
			if(hash2.containsKey(split[2])){
				t = hash2.get(split[2]);
				by2[byte_index] = (byte) (t/256);
				by2[byte_index+1] = (byte) (t%256);
			}if(hash3.containsKey(split[3])){
				t = hash3.get(split[3]);
				//by3[byte_index] = (byte) (t/256);
				by3[byte_index/2] = (byte) t;
			}
			//��2��Ϊ�˴����������ֽڴ�����ŵ����
			byte_index+=2;
			str = br.readLine();
		}
		System.out.println(System.currentTimeMillis());
		System.out.println("��ʼд�������ļ�");
		FileOutputStream f1 = new FileOutputStream("D:\\ѧУ\\ѧϰ\\JAVAʵ������ض���\\������������\\CUFE\\new data\\λͼ��ȡ�ļ�0\\atr1.bin");
		f1.write(by1, 0, byte_index/2);
		f1.flush();
		f1.close();
		FileOutputStream f2 = new FileOutputStream("D:\\ѧУ\\ѧϰ\\JAVAʵ������ض���\\������������\\CUFE\\new data\\λͼ��ȡ�ļ�0\\atr2.bin");
		f2.write(by2, 0, byte_index);
		f2.flush();
		f2.close();
		FileOutputStream f3 = new FileOutputStream("D:\\ѧУ\\ѧϰ\\JAVAʵ������ض���\\������������\\CUFE\\new data\\λͼ��ȡ�ļ�0\\atr3.bin");
		f3.write(by3, 0, byte_index/2);
		f3.flush();
		f3.close();
		System.out.println(System.currentTimeMillis());
		System.out.println("���������ɹ�");
		System.out.println("����ʹ�ÿ�ʼ");
		System.out.println(System.currentTimeMillis());
		File file1 = new File("D:\\ѧУ\\ѧϰ\\JAVAʵ������ض���\\������������\\CUFE\\new data\\λͼ��ȡ�ļ�0\\atr1.bin");
		FileInputStream fis = new FileInputStream(file1);
		FileChannel fc = fis.getChannel();
		//DataInputStream dis = new DataInputStream(fis);
		ByteBuffer byteBuffer = ByteBuffer.allocate((int)fc.size());
		while((fc.read(byteBuffer)) > 0){				
			
		}
		byte[] b = byteBuffer.array();
		int[] int1 = byte_to_int1(by1);
		File file2 = new File("D:\\ѧУ\\ѧϰ\\JAVAʵ������ض���\\������������\\CUFE\\new data\\λͼ��ȡ�ļ�0\\atr2.bin");
		fis = new FileInputStream(file2);
		fc = fis.getChannel();
		byteBuffer = ByteBuffer.allocate((int)fc.size());
		while((fc.read(byteBuffer)) > 0){
			
		}
		b = byteBuffer.array();
		int[] int2 = byte_to_int2(by2);
		File file3 = new File("D:\\ѧУ\\ѧϰ\\JAVAʵ������ض���\\������������\\CUFE\\new data\\λͼ��ȡ�ļ�0\\atr3.bin");
		fis = new FileInputStream(file3);
		fc = fis.getChannel();
		byteBuffer = ByteBuffer.allocate((int)fc.size());
		while((fc.read(byteBuffer)) > 0){
			
		}
		b = byteBuffer.array();
		int[] int3 = byte_to_int1(by3);
		fc.close();
		fis.close();
		System.out.println(int1.length+" "+int2.length+" "+int3.length);
		//Hashtable sort_table = new Hashtable();
		TreeMap<Integer, Integer> tm = new TreeMap<Integer, Integer>();
		//ͨ��һ������ֵ������ֵ��Ŷ�Ӧ��hashtable�Ϳ���ͨ�����Ե���ŵõ����Ե�ֵ
		for(int i = 0;i<int3.length;i++){
			tm.put(int1[i]*amountOfatr2*amountOfatr3+int2[i]*amountOfatr3+int3[i],i+1);
		}
		System.out.println(System.currentTimeMillis());
		File f = new File("D:\\ѧУ\\ѧϰ\\JAVAʵ������ض���\\������������\\CUFE\\new data\\λͼ��ȡ�ļ�0\\result_index.txt");
		//�ñ����ķ�ʽ��TreeMap��Ԫ�������result_index��txt��
		BufferedWriter output = new BufferedWriter(new FileWriter(f));
		
		Iterator it = tm.keySet().iterator();  
		System.out.println("the first is :" + tm.firstEntry());
        while (it.hasNext()) {  
            //it.next()�õ�����key��tm.get(key)�õ�obj  
        	output.write(tm.get(it.next())+"\n");  
        }
		output.close();
		System.out.println(System.currentTimeMillis());
		System.out.println("Deal with position:");
		System.out.print("Please input the value of journal_id:");
		Scanner in = new Scanner(System.in);
		String journal_id = in.nextLine();
		System.out.println(System.currentTimeMillis());
		int value = hash1.get(journal_id);
		int count=0;
		for(int i=0;i<int3.length;i++){
			if(int3[i]==value){
				count++;
			}
		}
		for(int i=0;i<int3.length;i++){
			if(int3[i]==15){
				count++;
			}
		}
		for(int i=0;i<int3.length;i++){
			if(int3[i]==15){
				count++;
			}
		}
		System.out.println(System.currentTimeMillis());
		System.out.println("hits "+count+ " cords");
		
	}
	public static int[] byte_to_int1(byte[] by){
		int[] re = new int[by.length];
		for(int i = 0;i<by.length;i++){
			re[i] = (int)(by[i]&0xff);
		}
		return re;
	}
	public static int[] byte_to_int2(byte[] by){
		int[] re = new int[by.length/2];
		for(int i = 0;i<by.length;i++){
			re[i/2] = (int)(by[i++]&0xFF)*256+(int)(by[i]&0xff);
		}
		return re;
	}
}

