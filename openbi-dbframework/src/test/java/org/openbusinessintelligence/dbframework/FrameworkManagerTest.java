package org.openbusinessintelligence.dbframework;
import static org.junit.Assert.*;

import org.junit.Test;

public class FrameworkManagerTest {

	@Test
	public void test() {
		// Perform test
		try {
			FrameworkManager fm = new FrameworkManager();
			fm.setFrameworkClass("org.openbusinessintelligence.testframework.TestFrameworkImpl");
			fm.getName();
		}
		catch (Exception e) {
			fail("Exception: \n" + e);
		}
	}

}
