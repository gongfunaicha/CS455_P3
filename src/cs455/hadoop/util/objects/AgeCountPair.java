package cs455.hadoop.util.objects;

// Used to map age range to count for renter age distribution problem
public class AgeCountPair implements Comparable<AgeCountPair>{
    private String ageRange;
    private long count;

    // No empty constructor offered


    // Index maps to age range in the order specified by the government document, should be between 0 and 6
    public AgeCountPair(int index, long count) {
        switch (index)
        {
            case 0:
                ageRange = "15 to 24 years";
                break;
            case 1:
                ageRange = "25 to 34 years";
                break;
            case 2:
                ageRange = "35 to 44 years";
                break;
            case 3:
                ageRange = "44 to 54 years";
                break;
            case 4:
                ageRange = "55 to 64 years";
                break;
            case 5:
                ageRange = "65 to 74 years";
                break;
            case 6:
                ageRange = "75 years and over";
                break;
            default:
                ageRange = "Unknown";
        }

        this.count = count;
    }

    // Comparator


    @Override
    public int compareTo(AgeCountPair ageCountPair) {
        long countCompare = ageCountPair.getCount();

        // Use this way due to both count are long type
        if (countCompare - this.getCount() > 0)
            return 1;
        else if ((countCompare - this.getCount()) == 0)
            return 0;
        else
            return -1;
    }

    public String getAgeRange() {
        return ageRange;
    }

    public long getCount() {
        return count;
    }

    public String getStringPercentage(double totalRenterCount)
    {
        double percentage = (count / totalRenterCount) * 100;

        return String.format("%.2f", percentage) + "%";
    }
}
