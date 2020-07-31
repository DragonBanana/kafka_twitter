package kafka.rest;

import kafka.db.AzureDBConn;
import kafka.model.Offset;
import kafka.model.OffsetKey;

public class OffsetStub {

    public void save(Offset offset) throws Exception {
        new AzureDBConn().put(offset);
    }

    public Offset get(OffsetKey key) throws Exception {
        return new AzureDBConn().get(key);
    }

}
