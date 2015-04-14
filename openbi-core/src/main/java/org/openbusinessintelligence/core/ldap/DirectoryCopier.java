package org.openbusinessintelligence.core.ldap;

import java.util.*;
import javax.naming.*;
import javax.naming.directory.*;

import org.slf4j.LoggerFactory;


public class DirectoryCopier {

	static final org.slf4j.Logger logger = LoggerFactory.getLogger(DirectoryCopier.class);

    // Declarations of bean properties
	// Source properties
	private String sourceURL = "";
	private String sourcePrincipal = "";
	private String sourcePassword = "";
	private String sourceFolder = "";
	
	
	// Target properties
	private String targetURL = "";
	private String targetPrincipal = "";
	private String targetPassword = "";
	private String targetFolder = "";
	
	// Default properties
	private String initContextFactory = "com.sun.jndi.ldap.LdapCtxFactory";
	private String securityAuth = "simple";
	
	private DirContext sourceCtx;
	private DirContext targetCtx;

	private Hashtable<String,String> env;
	private static BasicAttributes attribs;
	private static BasicAttribute ou,cn,objectClass,sn,givenName,uid,userPassword,mail;
	private static String sOu,sCn,sSn,sGivenName,sUid,sUserPassword,sMail;
	private static String[] attrIDs = {"cn","uid","sn","givenName","userPassword","mail"};
	private static String[] oUattrIDs = {"ou"};

	public void openSourceCtx() {

		// Set up environment for creating initial Domino context
		env = new Hashtable<String,String>(11);
		env.put(Context.INITIAL_CONTEXT_FACTORY, initContextFactory);
		env.put(Context.PROVIDER_URL, sourceURL);
		env.put(Context.SECURITY_AUTHENTICATION, securityAuth);
		env.put(Context.SECURITY_PRINCIPAL, sourcePrincipal);
		env.put(Context.SECURITY_CREDENTIALS, sourcePassword);

		try {
			sourceCtx = new InitialDirContext(env);
			sourceCtx = (DirContext)sourceCtx.lookup(sourceFolder);
		}
		catch (NamingException e) {
			e.printStackTrace();
		}
	}

	public void closeSourceCtx() {

		try {
			sourceCtx.close();
		}
		catch (NamingException e) {
			e.printStackTrace();
		}
	}

	public void openTargetCtx(String folder) {

		// Set up environment for creating initial Netscape context
		env = new Hashtable<String,String>(11);
		env.put(Context.INITIAL_CONTEXT_FACTORY, initContextFactory);
		env.put(Context.PROVIDER_URL, targetURL);
		env.put(Context.SECURITY_AUTHENTICATION, securityAuth);
		env.put(Context.SECURITY_PRINCIPAL, targetPrincipal);
		env.put(Context.SECURITY_CREDENTIALS, targetPassword);
		try {
			targetCtx = new InitialDirContext(env);
			targetCtx = (DirContext)targetCtx.lookup(targetFolder);

		}
		catch (NamingException e) {
			e.printStackTrace();
		}
	}

	public void closeTargetCtx() {

		try {
			targetCtx.close();
		}
		catch (NamingException e) {
			e.printStackTrace();
		}
	}

	public void export() {

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
		}
		catch (NamingException e) {
			e.printStackTrace();
		}
	}
}
