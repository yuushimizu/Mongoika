package mongoika;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

/**
 * A class to fixing a bug of GridFS#getFileList.
 *
 * GridFS#getFileList does not set a GridFS in fetched files.
 * A GridFS must be set in a GridFSDBFile when read data from it.
 */
public class GridFSDBFileSettable extends GridFSDBFile {
    public GridFSDBFileSettable(final GridFS gridFS, final GridFSDBFile source) {
        this.setGridFS(gridFS);
        this._mongoika_copyFrom(source);
    }

    public void setGridFS(final GridFS gridFS) {
        super.setGridFS(gridFS);
    }

    public void _mongoika_copyFrom(final GridFSDBFile source) {
        // GridFSDBFile does not support putAll().
        for (final String key : source.keySet()) super.put(key, source.get(key));
    }
}
