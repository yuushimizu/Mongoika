package mongoika;
import clojure.lang.IPersistentMap;
import clojure.lang.LazySeq;
import clojure.lang.IObj;
import clojure.lang.ISeq;
import clojure.lang.IPersistentCollection;
import clojure.lang.Counted;
import clojure.lang.Sequential;
import clojure.lang.IPending;
import clojure.lang.IMeta;
import java.util.List;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Collection;

public class QuerySequence implements IObj, Counted, Sequential, ISeq, List<Object>, IPending {
    private final IPersistentMap meta;
    private final LazySeq documentsLazySequence;
    private final Object properMongoDBCollection;
    private final IPersistentMap parameters;
    private final ProperMongoDBCollectionAdapter properMongoDBCollectionAdapter;

    public QuerySequence(final IPersistentMap meta, final LazySeq documentsLazySequence, final Object properMongoDBCollection, final IPersistentMap parameters, final ProperMongoDBCollectionAdapter properMongoDBCollectionAdapter) {
        this.meta = meta;
        this.documentsLazySequence = documentsLazySequence;
        this.properMongoDBCollection = properMongoDBCollection;
        this.parameters = parameters;
        this.properMongoDBCollectionAdapter = properMongoDBCollectionAdapter;
    }

    public Object properMongoDBCollection() {
        return this.properMongoDBCollection;
    }

    public IPersistentMap parameters() {
        return this.parameters;
    }

    public int hashCode() {
        return this.documentsLazySequence.hashCode();
    }

    public boolean equels(final Object other) {
        return this.documentsLazySequence.equals(other);
    }

    public IPersistentMap meta() {
        return this.meta;
    }

    public IObj withMeta(final IPersistentMap meta) {
        return new QuerySequence(meta, this.documentsLazySequence, this.properMongoDBCollection, this.parameters, this.properMongoDBCollectionAdapter);
    }

    public int count() {
        if (this.documentsLazySequence.isRealized()) {
            return this.documentsLazySequence.count();
        } else {
            return this.properMongoDBCollectionAdapter.countDocuments(this.properMongoDBCollection, this.parameters);
        }
    }

    public ISeq seq() {
        return this.documentsLazySequence.seq();
    }

    public IPersistentCollection empty() {
        return this.documentsLazySequence.empty();
    }

    public boolean equiv(final Object object) {
        return this.documentsLazySequence.equiv(object);
    }

    public Object first() {
        return this.documentsLazySequence.first();
    }

    public ISeq next() {
        return this.documentsLazySequence.next();
    }

    public ISeq more() {
        return this.documentsLazySequence.more();
    }

    public ISeq cons(final Object object) {
        return this.documentsLazySequence.cons(object);
    }

    public Object[] toArray() {
        return this.documentsLazySequence.toArray();
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toArray(final T[] array) {
        return (T[]) this.documentsLazySequence.toArray(array);
    }

    public boolean remove(final Object object) {
        return this.documentsLazySequence.remove(object);
    }

    public Object remove(final int index) {
        return this.documentsLazySequence.remove(index);
    }

    public void clear() {
        this.documentsLazySequence.clear();
    }

    public boolean retainAll(final Collection<?> collection) {
        return this.documentsLazySequence.retainAll(collection);
    }

    public boolean removeAll(final Collection<?> collection) {
        return this.documentsLazySequence.removeAll(collection);
    }

    public boolean containsAll(final Collection<?> collection) {
        return this.documentsLazySequence.containsAll(collection);
    }

    public int size() {
        return this.documentsLazySequence.size();
    }

    public boolean isEmpty() {
        return this.documentsLazySequence.isEmpty();
    }

    public boolean contains(final Object object) {
        return this.documentsLazySequence.contains(object);
    }

    @SuppressWarnings("unchecked")
    public Iterator<Object> iterator() {
        return (Iterator<Object>) this.documentsLazySequence.iterator();
    }

    @SuppressWarnings("unchecked")
    public List<Object> subList(final int from, final int to) {
        return (List<Object>)this.documentsLazySequence.subList(from, to);
    }

    public Object set(final int index, final Object object) {
        return this.documentsLazySequence.set(index, object);
    }

    public int indexOf(final Object object) {
        return this.documentsLazySequence.indexOf(object);
    }

    public int lastIndexOf(final Object object) {
        return this.documentsLazySequence.lastIndexOf(object);
    }

    @SuppressWarnings("unchecked")
    public ListIterator<Object> listIterator() {
        return (ListIterator<Object>) this.documentsLazySequence.listIterator();
    }

    @SuppressWarnings("unchecked")
    public ListIterator<Object> listIterator(final int index) {
        return (ListIterator<Object>) this.documentsLazySequence.listIterator(index);
    }

    public Object get(final int index) {
        return this.documentsLazySequence.get(index);
    }

    public boolean add(final Object object) {
        return this.documentsLazySequence.add(object);
    }
    public void add(final int index, final Object object) {
        this.documentsLazySequence.add(index, object);
    }
    
    public boolean addAll(final int index, final Collection<?> collection) {
        return this.documentsLazySequence.addAll(index, collection);
    }

    public boolean addAll(final Collection<?> collection) {
        return this.documentsLazySequence.addAll(collection);
    }

    public boolean isRealized() {
        return this.documentsLazySequence.isRealized();
    }
}
