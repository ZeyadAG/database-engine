import java.util.Hashtable;

public class Main {
    public static void main(String[] args) throws Exception {
        DBApp engine = new DBApp();
        engine.init();

        // create table
        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> mins = new Hashtable<String, String>();
        mins.put("id", "0");
        mins.put("name", "A");
        mins.put("gpa", "0.0");

        Hashtable<String, String> maxs = new Hashtable<String, String>();
        maxs.put("id", "9999");
        maxs.put("name", "ZZZZZZZZZZZZZZZZ");
        maxs.put("gpa", "4.0");

        // insert data
        Hashtable<String, Object> t1 = new Hashtable<String, Object>();
        t1.put("id", 1);
        t1.put("gpa", 2.0);
        t1.put("name", "Stafa");

        Hashtable<String, Object> t2 = new Hashtable<String, Object>();
        t2.put("id", 2);
        t2.put("gpa", 2.0);
        t2.put("name", "Sdaf");

        Hashtable<String, Object> t3 = new Hashtable<String, Object>();
        t3.put("id", 3);
        t3.put("gpa", 3.0);
        t3.put("name", "Sertaf");

        Hashtable<String, Object> t4 = new Hashtable<String, Object>();
        t4.put("id", 4);
        t4.put("gpa", 3.0);
        t4.put("name", "Seaf");

        Hashtable<String, Object> t5 = new Hashtable<String, Object>();
        t5.put("id", 5);
        t5.put("gpa", 3.0);
        t5.put("name", "Sesdfaf");

        Hashtable<String, Object> t6 = new Hashtable<String, Object>();
        t6.put("id", 6);
        t6.put("gpa", 3.0);
        t6.put("name", "Sesdfaf");

        Hashtable<String, Object> t7 = new Hashtable<String, Object>();
        t7.put("id", 16);
        t7.put("gpa", 3.0);

        // delete data
        Hashtable<String, Object> d1 = new Hashtable<String, Object>();
        d1.put("name", "Sesdfaf");

        Hashtable<String, Object> d2 = new Hashtable<String, Object>();
        d2.put("gpa", 3.0);
        d2.put("name", "Seaf");

        // update data
        Hashtable<String, Object> u1 = new Hashtable<String, Object>();
        u1.put("name", "Sssss");

        Hashtable<String, Object> u2 = new Hashtable<String, Object>();
        u2.put("name", "Sesdfaf");

        Hashtable<String, Object> u3 = new Hashtable<String, Object>();
        u3.put("name", 4);

        // engine

        // engine.createTable("Student", "id", htblColNameType, mins, maxs);

        // engine.insertIntoTable("Student", t1);
        // engine.insertIntoTable("Student", t2);
        // engine.insertIntoTable("Student", t3);
        // engine.insertIntoTable("Student", t4);
        // engine.insertIntoTable("Student", t5);
        // engine.insertIntoTable("Student", t6);

        // engine.insertIntoTable("Student", t7);

        // engine.deleteFromTable("Student", d1);

        // engine.updateTable("Student", "2", u3);

        String[] colNames = { "gpa", "id", "name" };

        engine.createIndex("Student", colNames);

        OctTreeIndex oti = (OctTreeIndex) engine.readObject("resources/indices/gpa_id_name_Index.class");

        System.out.println("max entries: " + oti.octTree.maxEntries);

        Cube bounds = oti.octTree.bounds;
        System.out.println("minC1: " + bounds.minC1 + ", maxC1: " + bounds.maxC1);
        System.out.println("minC2: " + bounds.minC2 + ", maxC2: " + bounds.maxC2);
        System.out.println("minC3: " + bounds.minC3 + ", maxC3: " + bounds.maxC3);

        // engine.clear();

        // print page content
        System.out.println("\nin main");
        TableInfo tableInfo = (TableInfo) engine.readObject("Student" + "Info" +
                ".class");
        engine.printPagesContent(tableInfo);

        // t1,

    }

}
