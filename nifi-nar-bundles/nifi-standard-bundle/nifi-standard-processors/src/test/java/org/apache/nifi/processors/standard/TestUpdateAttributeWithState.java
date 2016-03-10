package org.apache.nifi.processors.standard;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestUpdateAttributeWithState {


    @Test
    public void testBasic(){
        final TestRunner runner = TestRunners.newTestRunner(UpdateAttributeWithState.class);

        runner.setProperty(UpdateAttributeWithState.ATTRIBUTE_NAME, "theCount");
        runner.setProperty(UpdateAttributeWithState.ATTRIBUTE_VALUE_TO_SET, "${count}");
        runner.setProperty("1count", "${count:plus(1)}");
        runner.setProperty("2count", "${count:plus(count:plus(1))}");
        runner.setProperty("3count", "${count:plus(1)}");

        runner.enqueue("1".getBytes());
        runner.enqueue("2".getBytes());
        runner.enqueue("3".getBytes());
        runner.enqueue("4".getBytes());
        runner.run(4);

        runner.assertAllFlowFilesTransferred(CompressContent.REL_SUCCESS, 4);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(CompressContent.REL_SUCCESS).get(3);
        flowFile.assertAttributeEquals("theCount", "4");
    }

}