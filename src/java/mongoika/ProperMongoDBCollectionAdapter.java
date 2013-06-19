package mongoika;
import clojure.lang.IPersistentMap;
import clojure.lang.LazySeq;

public interface ProperMongoDBCollectionAdapter {
    public LazySeq makeDocumentsLazySequence(final Object properMongoDBCollection, final IPersistentMap parameters);
    public int countDocuments(final Object properMongoDBCollection, final IPersistentMap parameters);
}
