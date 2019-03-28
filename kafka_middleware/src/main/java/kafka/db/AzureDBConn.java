package kafka.db;

// Include the following imports to use table APIs
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.table.*;
import com.microsoft.azure.storage.table.TableQuery.*;
import kafka.model.Offset;
import kafka.model.OffsetKey;
import kafka.model.OffsetValue;

public class AzureDBConn {

    /**
     * The storage connection string. It is used to connect to the Azure Table Storage service.
     */
    public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=kafkastoragemiddleware;AccountKey=NqOrGc0IlR9VpFWlneipV6mPZ1P8wlWoyoM1OL9/tSpI8n1omW6TPH7K8NSl+UsXi2B1JzD4yKjnL9iVacIKpQ==;EndpointSuffix=core.windows.net";
    /**
     * The table name of the table in the Azure Table Storage service.
     */
    public static final String tableName = "kafkatablemiddleware";

    /**
     * Save an offset in the Azure Table.
     * @param offset the offset to be saved.
     * */
    public void put(Offset offset){
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);
            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();
            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference(tableName);
            // Create a new offset entity.
            OffsetEntity o = new OffsetEntity(offset);
            // Create an operation to add the new offset to the offset table.
            TableOperation insertCustomer1 = TableOperation.insertOrReplace(o);
            // Submit the operation to the table service.
            cloudTable.execute(insertCustomer1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Retrieve an offset in the Azure Table.
     * @param key the key of the offset that has to be retrieved.
     */
    public Offset get(OffsetKey key){
        try {
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount =
                    CloudStorageAccount.parse(storageConnectionString);
            // Create the table client.
            CloudTableClient tableClient = storageAccount.createCloudTableClient();
            // Create a cloud table object for the table.
            CloudTable cloudTable = tableClient.getTableReference(tableName);
            // Create a cloud table object for the table.
            TableOperation tableOperation =
                    TableOperation.retrieve(key.getUser(), key.getUser() + key.getTopicPartition() + key.getFilter().hashCode(), OffsetEntity.class);
            // Submit the operation to the table service and get the specific entity.
            OffsetEntity entity = cloudTable.execute(tableOperation).getResultAsType();
            if(entity == null) {
                entity = new OffsetEntity(new Offset(key, new OffsetValue(0)));
            }
            return entity.asOffset();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //TODO
    //Da rimuovere, solo per testing
    public static void main(String[] args) {
        try {
            new AzureDBConn().put(new Offset("user", "filter", 5, 1005L));
            new AzureDBConn().put(new Offset("user", "filter", 4, 1006L));
            Offset o = new AzureDBConn().get(new OffsetKey("user", "filter", 4));
            System.out.println(o.getKey().getUser() + o.getKey().getFilter() + o.getValue().getOffset());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
