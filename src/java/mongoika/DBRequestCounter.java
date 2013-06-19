package mongoika;
import com.mongodb.DB;

public class DBRequestCounter {
    public class Frame {
        private boolean poped;
        public Frame() {
            DBRequestCounter.this.increment();
            this.poped = false;
        }

        public void pop() {
            synchronized (this) {
                if (!this.poped) {
                    this.poped = true;
                    DBRequestCounter.this.decrement();
                }
            }
        }

        public void finalize() {
            this.pop();
        }
    }
    
    private final DB db;
    private int requestCount;

    public DBRequestCounter(final DB db) {
        this.db = db;
        this.requestCount = 0;
    }

    public synchronized void increment() {
        if (this.requestCount == 0) {
            this.db.requestStart();
        }
        this.requestCount += 1;
    }

    public synchronized void decrement() {
        this.requestCount -= 1;
        if (this.requestCount == 0) this.db.requestDone();
    }

    public Frame newFrame() {
        return new Frame();
    }
}
