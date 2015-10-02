package seglo;

import java.util.List;

public class TestAction implements Runnable {

    public final static String Name = "TestAction";

    private final Integer i;
    private final Double d;
    private final Boolean b;
    private final String s;
    private final List<Object> l;

    public TestAction(final Integer i, final Double d, final Boolean b, final String s, final List<Object> l) {
        this.i = i;
        this.d = d;
        this.b = b;
        this.s = s;
        this.l = l;
    }

    public void run() {
        System.out.println(String.format("Received message: TestAction.run() %s %s %s %s %s", this.i, this.d, this.b, this.s, this.l));
    }
}
