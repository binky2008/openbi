package org.openbusinessintelligence.cli;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.openbusinessintelligence.cli.copy.table.MainTestCopyTableFromMySQL;

@RunWith(Suite.class)
@SuiteClasses({MainTestDBProperties.class, MainTestCopyTableFromMySQL.class})

public class AllTests {

}
