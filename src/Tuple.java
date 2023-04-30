import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable {
	Hashtable<String, Object> data;

	public Tuple(Hashtable<String, Object> t) {
		this.data = t;
	}
}
