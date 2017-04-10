package cs455.hadoop.util;

import cs455.hadoop.util.objects.AgeDistributionObject;
import cs455.hadoop.util.objects.HousePositionCountObject;
import cs455.hadoop.util.objects.MarriageCountObject;
import cs455.hadoop.util.objects.ResidenceCountObject;

// Class providing data extraction static methods of all kinds of data
// Precondition: the line in the right partition
public class DataExtractor {
    public static ResidenceCountObject residenceCountExtractor(String line)
    {
        // Index 1804 - 1812 is the number of owner occupied house
        long ownedCount = Long.parseLong(line.substring(1803, 1812));

        // Index 1813 - 1821 is the number of renter occupied house
        long rentCount = Long.parseLong(line.substring(1812, 1821));

        return new ResidenceCountObject(rentCount, ownedCount);
    }

    public static MarriageCountObject marriageCountExtractor(String line)
    {
        // Index 4423 - 4431 is Male: Never Married
        long maleNeverMarried = Long.parseLong(line.substring(4422,4431));

        // Index 364 - 372 is male total population
        long maleTotal = Long.parseLong(line.substring(363,372));

        // Index 4468 - 4476 is Female: Never Married
        long femaleNeverMarried = Long.parseLong(line.substring(4467,4476));

        // Index 373 - 381 is female total population
        long femaleTotal = Long.parseLong(line.substring(372,381));

        return new MarriageCountObject(maleNeverMarried, maleTotal, femaleNeverMarried, femaleTotal);
    }

    public static AgeDistributionObject ageDistributionExtractor(String line)
    {
        long male_18 = 0;
        // Loop from Male: Under 1 year to Male: 18 years to calculate number of male under 18 (inclusive)
        // Loop 13 times due to 13 entries
        for (int i = 0; i < 13; i++)
        {
            // Index 3865 - 3873 is Male: Under 1 year
            male_18 += Long.parseLong(line.substring(3864 + 9 * i, 3873 + 9 * i));
        }

        long male19_29 = 0;
        // Loop from Male: 19 years to Male: 25 to 29 years to calculate number of male from 19 to 29 (inclusive)
        // Loop 5 times due to 5 entries
        for (int i = 0; i < 5; i++)
        {
            // Index 3982 - 3990 is Male: 19 years
            male19_29 += Long.parseLong(line.substring(3981 + 9 * i, 3990 + 9 * i));
        }

        long male30_39 = 0;
        // Loop from Male: 30 to 34 years to Male: 35 to 39 years to calculate number of male from 30 to 39 (inclusive)
        // Loop 2 times due to 2 entries
        for (int i = 0; i < 2; i++)
        {
            // Index 4027 - 4035 is Male: 30 to 34 years
            male30_39 += Long.parseLong(line.substring(4026 + 9 * i, 4035 + 9 * i));
        }

        long male40_ = 0;
        // Loop from Male: 40 to 44 years to Male: 85 years and over to calculate number of male above 40 (inclusive)
        // Loop 11 times due to 11 entries
        for (int i = 0; i < 11; i++)
        {
            // Index 4045 - 4053 is Male: 40 to 44 years
            male40_ += Long.parseLong(line.substring(4044 + 9 * i, 4053 + 9 * i));
        }

        long female_18 = 0;
        // Loop from Female: Under 1 year to Female: 18 years to calculate number of female under 18 (inclusive)
        // Loop 13 times due to 13 entries
        for (int i = 0; i < 13; i++)
        {
            // Index 4144 - 4152 is Female: Under 1 year
            female_18 += Long.parseLong(line.substring(4143 + 9 * i, 4152 + 9 * i));
        }

        long female19_29 = 0;
        // Loop from Female: 19 years to Female: 25 to 29 years to calculate number of female from 19 to 29 (inclusive)
        // Loop 5 times due to 5 entries
        for (int i = 0; i < 5; i++)
        {
            // Index 4261 - 4269 is Female: 19 years
            female19_29 += Long.parseLong(line.substring(4260 + 9 * i, 4269 + 9 * i));
        }

        long female30_39 = 0;
        // Loop from Female: 30 to 34 years to Female: 35 to 39 years to calculate number of female from 30 to 39 (inclusive)
        // Loop 2 times due to 2 entries
        for (int i = 0; i < 2; i++)
        {
            // Index 4306 - 4314 is Male: 30 to 34 years
            female30_39 += Long.parseLong(line.substring(4305 + 9 * i, 4314 + 9 * i));
        }

        long female40_ = 0;
        // Loop from Female: 40 to 44 years to Female: 85 years and over to calculate number of female above 40 (inclusive)
        // Loop 11 times due to 11 entries
        for (int i = 0; i < 11; i++)
        {
            // Index 4324 - 4332 is Male: 40 to 44 years
            female40_ += Long.parseLong(line.substring(4323 + 9 * i, 4332 + 9 * i));
        }

        return new AgeDistributionObject(male_18, male19_29, male30_39, male40_, female_18, female19_29, female30_39, female40_);
    }

    public static HousePositionCountObject housePositionCountExtractor(String line)
    {
        // Index 1822 - 1830 is Urban: Inside urbanized area
        long urbanCount = Long.parseLong(line.substring(1821, 1830));

        // Index 1831 - 1839 is Urban: Outside urbanized area
        urbanCount += Long.parseLong(line.substring(1830, 1839));

        // Index 1840 - 1848 is Rural
        long ruralCount = Long.parseLong(line.substring(1839, 1848));

        // Index 1849 - 1857 is Not defined for this file
        long otherCount = Long.parseLong(line.substring(1848, 1857));

        return new HousePositionCountObject(urbanCount, ruralCount, otherCount);
    }

}
