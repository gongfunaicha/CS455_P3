package cs455.hadoop.util.objects;

public class ResidenceCountObject {
    private long rentCount;
    private long ownedCount;

    public ResidenceCountObject() {
        rentCount = 0;
        ownedCount = 0;
    }

    public ResidenceCountObject(long rentCount, long ownedCount) {
        this.rentCount = rentCount;
        this.ownedCount = ownedCount;
    }

    public long getRentCount() {
        return rentCount;
    }

    public long getOwnedCount() {
        return ownedCount;
    }
}
