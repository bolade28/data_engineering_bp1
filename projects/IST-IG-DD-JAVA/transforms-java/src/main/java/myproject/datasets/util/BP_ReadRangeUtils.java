package myproject.datasets.util;

import com.palantir.transforms.lang.java.api.FoundryInput;
import com.palantir.transforms.lang.java.api.ReadRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is an example high-level Transform intended for automatic registration. */
public final class BP_ReadRangeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BP_ReadRangeUtils.class);
    
    public static ReadRange getReadRange(FoundryInput input) {
        switch (input.asDataFrame().modificationType()) {
            case UNCHANGED:
                LOG.info("No changes in input dataset, read only unprocessed");
                return ReadRange.UNPROCESSED;
            case APPENDED:
                LOG.info("Append-only changes in input dataset, read only unprocessed");
                return ReadRange.UNPROCESSED;
            case UPDATED:
                LOG.info("Update-type changes in input dataset, read entire view");
                return ReadRange.ENTIRE_VIEW;
            case NEW_VIEW:
                LOG.info("New view in input dataset, read entire view");
                return ReadRange.ENTIRE_VIEW;
            default:
                throw new IllegalArgumentException(
                        "Unknown ModificationType for input dataset "
                                + input.asDataFrame().modificationType());
        }
    }
}
