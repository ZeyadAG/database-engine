import java.util.Enumeration;
import java.util.Hashtable;

public class TupleReference {
    Hashtable<String, Object> values;
    Object c1, c2, c3;
    String pageReference;

    public TupleReference(Hashtable<String, Object> values, String pageReference) {
        this.values = values;

        Enumeration<String> valuesE = values.keys();
        this.c3 = values.get(valuesE.nextElement());
        this.c2 = values.get(valuesE.nextElement());
        this.c1 = values.get(valuesE.nextElement());

        this.pageReference = pageReference;
    }

    public String toString() {
        return "{c1: " + c1 + ", c2: " + c2 + ", c3: " + c3 + "}";
    }
}
