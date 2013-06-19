package mongoika;
import clojure.lang.IPersistentMap;

public interface ProperMongoDBCollectionAdapter {
    public int countDocuments(final Object properMongoDBCollection, final IPersistentMap parameters);
}
