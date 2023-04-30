import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable{
	Vector<Tuple> tuples;
	public Page() {
		this.tuples=new Vector<Tuple>();
	}
}
