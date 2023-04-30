import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;

public class TableInfo implements Serializable {
	String tableName;
	String clusteringKeyName;
	ArrayList<String> clusteringKeyData;
	ArrayList<String> tablePages;
	int pageCount;

	public TableInfo() {
		this.clusteringKeyData = new ArrayList<String>();
		this.tablePages = new ArrayList<String>();
		this.pageCount = -1;
		this.clusteringKeyName = null;
		this.tableName = null;
	}

	public static void main(String[] args) throws ParseException {
		// Object x = new Date();
		String a = "1.0";
		String b = "1.2";
		String c = "3";
		String d = "4";

		// Date date1 = new SimpleDateFormat("YYYY-MM-DD").parse("2002-02-13");

		// Date date2 = new SimpleDateFormat("YYYY-MM-DD").parse("2004-02-15");

		// Date date3 = new SimpleDateFormat("YYYY-MM-DD").parse("2002-02-14");

		ArrayList<String> arr = new ArrayList<>();

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date d1 = format.parse("2003-2-2");
		String date1 = format.format(d1);
		Date d2 = format.parse("2004-2-2");
		String date2 = format.format(d2);
		Date d3 = format.parse("2003-2-3");
		String date3 = format.format(d3);

		arr.add(date1);
		arr.add(date2);
		arr.add(date3);

		// SimpleDateFormat format = new SimpleDateFormat("YYYYY-MM-DD");
		// Date d1 = format.parse("2002-02-14");
		// SimpleDateFormat dateFormatYouWant = new SimpleDateFormat("YYYYY-MM-DD");
		// String sCertDate = dateFormatYouWant.format(d1);
		// System.out.println(sCertDate);

		// arr.add(date.getTime() + "");
		// arr.add(date2.getTime() + "");
		// arr.add(date3.getTime() + "");

		System.out.println(arr);

		arr.sort(Comparator.naturalOrder());

		System.out.println(arr);
		// System.out.println(date + "");
	}
}