package kafka.db;

// Include the following imports to use table APIs
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.table.*;
import com.microsoft.azure.storage.table.TableQuery.*;
import kafka.model.Offset;
import kafka.model.OffsetKey;
import kafka.model.OffsetValue;

public class AzureDBConn {

    public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=kafkastoragemiddleware;AccountKey=NqOrGc0IlR9VpFWlneipV6mPZ1P8wlWoyoM1OL9/tSpI8n1omW6TPH7K8NSl+UsXi2B1JzD4yKjnL9iVacIKpQ==;EndpointSuffix=core.windows.net";
    public static final String tableName = "kafkatablemiddleware";

    public void put(Offset offset) throws Exception{
        // Retrieve storage account from connection-string.
        CloudStorageAccount storageAccount =
                CloudStorageAccount.parse(storageConnectionString);
        // Create the table client.
        CloudTableClient tableClient = storageAccount.createCloudTableClient();
        // Create a cloud table object for the table.
        CloudTable cloudTable = tableClient.getTableReference(tableName);
        // Create a new customer entity.
        OffsetEntity o = new OffsetEntity(offset);
        // Create an operation to add the new customer to the people table.
        TableOperation insertCustomer1 = TableOperation.insertOrReplace(o);
        // Submit the operation to the table service.
        cloudTable.execute(insertCustomer1);
    }

    public Offset get(OffsetKey key) throws Exception{
        // Retrieve storage account from connection-string.
        CloudStorageAccount storageAccount =
                CloudStorageAccount.parse(storageConnectionString);
        // Create the table client.
        CloudTableClient tableClient = storageAccount.createCloudTableClient();
        // Create a cloud table object for the table.
        CloudTable cloudTable = tableClient.getTableReference(tableName);
        // Create a cloud table object for the table.
        TableOperation tableOperation =
                TableOperation.retrieve(key.getUser(), key.getFilter(), OffsetEntity.class);

        // Submit the operation to the table service and get the specific entity.
        OffsetEntity entity =
                cloudTable.execute(tableOperation).getResultAsType();
        return entity.asOffset();
    }

}
