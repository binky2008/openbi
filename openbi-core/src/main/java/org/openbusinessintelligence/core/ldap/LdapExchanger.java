package org.openbusinessintelligence.core.ldap;

import java.util.*;
import javax.naming.*;
import javax.naming.directory.*;

import org.slf4j.LoggerFactory;


/**
 * @author Nicola Marangoni
 *
 * Start Frame
 */

public class LdapExchanger {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(LdapExchanger.class);

	private static DirContext sourceCtx;
	private static DirContext targetCtx;

	private static Hashtable<String,String> env;
	private static BasicAttributes attribs;
	private static BasicAttribute ou,cn,objectClass,sn,givenName,uid,userPassword,mail;
	private static String sOu,sCn,sSn,sGivenName,sUid,sUserPassword,sMail;
	private static String[] attrIDs = {"cn","uid","sn","givenName","userPassword","mail"};
	private static String[] oUattrIDs = {"ou"};

	public static void openSourceCtx(String folder) {

		// Set up environment for creating initial Domino context
		env = new Hashtable<String,String>(11);
		env.put(Context.INITIAL_CONTEXT_FACTORY,"com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, "ldap://localhost:389/dc=viaginterkom,dc=de");
		env.put(Context.SECURITY_AUTHENTICATION, "simple");
		env.put(Context.SECURITY_PRINCIPAL, "cn=Manager,dc=viaginterkom,dc=de");
		env.put(Context.SECURITY_CREDENTIALS, "admin1234");

		try {
			sourceCtx = new InitialDirContext(env);
			sourceCtx = (DirContext)sourceCtx.lookup(folder);
		}
		catch (NamingException e) {
			e.printStackTrace();
		}

	}

	public static void closeSourceCtx() {

		try {
			sourceCtx.close();
		}
		catch (NamingException e) {
			e.printStackTrace();
		}

	}

	public static void openTargetCtx(String folder) {

		// Set up environment for creating initial Netscape context
		env = new Hashtable<String,String>(11);
		env.put(Context.INITIAL_CONTEXT_FACTORY,"com.sun.jndi.ldap.LdapCtxFactory");
		env.put(Context.PROVIDER_URL, "ldap://localhost:10389/dc=viaginterkom,dc=de");
		env.put(Context.SECURITY_AUTHENTICATION, "simple");
		env.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
		env.put(Context.SECURITY_CREDENTIALS, "secret");
		try {
			targetCtx = new InitialDirContext(env);
			targetCtx = (DirContext)targetCtx.lookup(folder);

		}
		catch (NamingException e) {
			e.printStackTrace();
		}
	}

	public static void closeTargetCtx() {

		try {
			targetCtx.close();
		}
		catch (NamingException e) {
			e.printStackTrace();
		}

	}

	public static void export(String f) {

		try {

			Binding bd = null;
			String bdName = null;
			Attributes answer = null;
			Object userId = null;

			NamingEnumeration<Binding> bindings = sourceCtx.listBindings("");

			while (bindings.hasMore()) {

				bd = (Binding)bindings.next();
				bdName = bd.getName();
				System.out.println("***** bindingName= "+ bdName +" ********************");
				if (bdName.substring(0,2).toLowerCase().equals("ou")) {

					answer = sourceCtx.getAttributes(bdName,oUattrIDs);
					attribs = new BasicAttributes();
					ou = new BasicAttribute("ou");
					objectClass = new BasicAttribute("objectClass");

					sOu = answer.get("ou").get().toString();
					System.out.println("ou= "+ sOu);

					ou.add(sOu);
					objectClass.add("top");
					objectClass.add("organizationalunit");

					attribs.put(ou);
					attribs.put(objectClass);

					try {
						targetCtx.destroySubcontext("ou="+ sOu);
						System.out.println("Organizational Unit destroyed");
					}
					catch (NamingException e) {
						System.out.println("Organizational Unit cannot be destroyed");
						System.out.println(e);
					}
					try {
						targetCtx.createSubcontext("ou="+ sOu,attribs);
						System.out.println("Organizational Unit created");
					}
					catch (NamingException e) {
						System.out.println("Organizational Unit cannot be created");
						System.out.println(e);
					}
					System.out.println();

				}
				/*else if (bdName.toLowerCase().equals("cn=ldap admin")) {

				}*/
				else if (bdName.substring(0,2).toLowerCase().equals("cn")) {

					try {
						answer = sourceCtx.getAttributes(bdName,attrIDs);
						attribs = new BasicAttributes();
						cn = new BasicAttribute("cn");
						sn = new BasicAttribute("sn");
						givenName = new BasicAttribute("givenName");
						uid = new BasicAttribute("uid");
						userPassword = new BasicAttribute("userPassword");
						mail = new BasicAttribute("mail");
						objectClass = new BasicAttribute("objectClass");

						sCn = answer.get("cn").get().toString();
						sUid = answer.get("uid").get().toString();
						if (answer.get("sn")==null) {
							sSn = "";
						}
						else {
							sSn = answer.get("sn").get().toString();
						}
						if (answer.get("givenName")==null) {
							sGivenName = "";
						}
						else {
							sGivenName = answer.get("givenName").get().toString();
						}
						if (answer.get("userPassword")==null) {
							sUserPassword = "";
						}
						else {
							sUserPassword = answer.get("userPassword").get().toString();
						}


						System.out.println("cn= "+ sCn);
						System.out.println("uid= "+ sUid);
						System.out.println("userPassword= "+ sUserPassword);
						System.out.println("givenName= "+ sGivenName);
						System.out.println("sn= "+ sSn);
						System.out.println("mail= "+ sMail);

						cn.add(sCn);
						uid.add(sUid);
						sn.add(sSn);
						givenName.add(sGivenName);
						userPassword.add(sUserPassword);
						mail.add(sMail);
						objectClass.add("top");
						objectClass.add("person");
						objectClass.add("organizationalperson");
						objectClass.add("inetorgperson");

						attribs.put(userPassword);
						attribs.put(givenName);
						attribs.put(objectClass);
						attribs.put(sn);
						attribs.put(uid);
						attribs.put(cn);

						userId = sCn;
						try {
							targetCtx.destroySubcontext("cn="+ userId);
							System.out.println("User destroyed");
						}
						catch (NamingException e) {
							System.out.println("User cannot be destroyed");
						}
						try {
							targetCtx.createSubcontext("cn="+ userId,attribs);
							System.out.println("User created");
						}
						catch (NamingException e) {
							System.out.println("User cannot be created");
						}
						System.out.println();
					}
					catch (Exception e) {
						System.out.println(e);
					}
				}
			}

			System.out.println();
			System.out.println();
			System.out.println("*************** "+ f +" EXPORTED ***************");
			System.out.println();

		}
		catch (NamingException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		String[] folders = {
			""
		};

		for ( int i = 0; i < folders.length; i++ ) {
			openTargetCtx(folders[i]);
			openSourceCtx(folders[i]);
			export(folders[i]);
			closeSourceCtx();
			closeTargetCtx();
		}

		System.out.println();
		System.out.println();
		System.out.println("************************************************");
		System.out.println("*************** EXPORT COMPLETED ***************");
		System.out.println("************************************************");
		System.out.println();

	}
}