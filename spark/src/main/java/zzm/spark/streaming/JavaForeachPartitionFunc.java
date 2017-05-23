package zzm.spark.streaming;
import org.apache.spark.sql.Row;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;

public abstract class JavaForeachPartitionFunc extends AbstractFunction1<Iterator<Row>, BoxedUnit> implements Serializable {
    @Override
    public BoxedUnit apply(Iterator<Row> it) {
        call(it);
        return BoxedUnit.UNIT;
    }
    
    public abstract void call(Iterator<Row> it);
    
    
   
    
}