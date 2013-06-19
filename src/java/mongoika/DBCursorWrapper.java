package mongoika;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.io.Closeable;

public class DBCursorWrapper implements Iterator<DBObject>, Iterable<DBObject>, Closeable {
    private final DBCursor cursor;
    private DBRequestCounter.Frame requestCounterFrame;

    public DBCursorWrapper(final DBCursor cursor, final DBRequestCounter.Frame requestCounterFrame) {
        this.cursor = cursor;
        this.requestCounterFrame = requestCounterFrame;
    }

    public boolean hasNext() {
        if (this.cursor.hasNext()) return true;
        this.close();
        return false;
    }

    public DBObject next() {
        try {
            return this.cursor.next();
        } catch (final NoSuchElementException exception) {
            this.close();
            throw exception;
        }
    }

    public void remove() {
        this.cursor.remove();
    }

    public Iterator<DBObject> iterator() {
        return this;
    }
    
    public void close() {
        this.cursor.close();
        if (this.requestCounterFrame != null) this.requestCounterFrame.pop();
    }
            
    public void finalize() {
        this.close();
    }
}
