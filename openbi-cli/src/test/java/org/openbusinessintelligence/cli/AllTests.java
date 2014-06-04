package org.openbusinessintelligence.cli;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.openbusinessintelligence.cli.copy.table.MainTestCopyTableFromMySQL;
import org.openbusinessintelligence.cli.dbproperties.MainTestDBPropertiesMySQL;

@RunWith(Suite.class)
@SuiteClasses({MainTestDBPropertiesMySQL.class, MainTestCopyTableFromMySQL.class})

public class AllTests {

}
