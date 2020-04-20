package myproject.datasets.util;

import com.palantir.transforms.lang.java.api.FoundryInput;
import com.palantir.transforms.lang.java.api.WriteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is an example high-level Transform intended for automatic registration. */
public final class BP_WriteRangeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BP_ReadRangeUtils.class);
    
    public static WriteMode getWriteRange(FoundryInput input) {
      switch (input.asDataFrame().modificationType()) {
        case UNCHANGED:
            LOG.info("No changes in input dataset, writing in update mode");
            return WriteMode.UPDATE;
        case APPENDED:
            LOG.info("Append-only changes in input dataset, writing in update mode");
            return WriteMode.UPDATE;
        case UPDATED:
            LOG.info("Update-type changes in input dataset, writing in snapshot mode");
            return WriteMode.SNAPSHOT;
        case NEW_VIEW:
            LOG.info("new view in input dataset, writing in snapshot mode");
            return WriteMode.SNAPSHOT;
        default:
            throw new IllegalArgumentException("Unknown ModificationType for input dataset " + input.asDataFrame().modificationType());
        }
    }
}