package org.openbusinessintelligence.dblibrary;
import static org.junit.Assert.*;

import org.junit.Test;
import org.openbusinessintelligence.dblibrary.DBLibraryManager;

public class FrameworkManagerTest {

	@Test
	public void test() {
		// Perform test
		try {
			DBLibraryManager fm = new DBLibraryManager();
			fm.setFrameworkClass("org.openbusinessintelligence.testframework.TestFrameworkImpl");
			fm.getName();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

}
