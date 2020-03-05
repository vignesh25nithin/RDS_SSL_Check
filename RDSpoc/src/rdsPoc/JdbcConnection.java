package rdsPoc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.google.common.io.BaseEncoding;

public class JdbcConnection {
	public static final String LINE_SEPARATOR = System.getProperty("line.separator");
	public static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----\n";
	public static final String END_CERT = "\n-----END CERTIFICATE-----\n";
	public static final int LINE_LENGTH = 64;
	public static final String dbName = "dbname";
	public static final String userName = "username";
	public static final String password = "userpass";
	public static final String classForName = "org.postgresql.Driver";
	public static final String hostname = "hostname";
	public static final String port = "5432";
	public static final String query = "SELECT * FROM public.test_table";

	private static void getRemoteConnectionWithoutSSL() {

		try {
			Statement stmt = null;
			Class.forName(classForName);
			String jdbcUrl = "jdbc:postgresql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName
					+ "&password=" + password;
			Connection con = DriverManager.getConnection(jdbcUrl);
			System.out.println("Remote connection successful --> getRemoteConnectionWithoutSSL");
			stmt = null != con ? con.createStatement() : null;
			if (null != stmt) {
				ResultSet rs = stmt.executeQuery(query);
				System.out.println("table data fetched");
				while (rs.next()) {
					System.out.println("data :" + rs.getString("data") + "   " + "data1 :" + rs.getString("data_one"));
				}
			}
			stmt.close();
		} catch (ClassNotFoundException e) {
			System.out.println(e.toString());
		} catch (SQLException e) {
			System.out.println(e.toString());
		}
	}
	
	/*
	 * https://github.com/pgjdbc/pgjdbc/issues/1307
	 */

	private static void getRemoteConnectionWithSSLLibPQFactory()
			throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {

		try {

			Statement stmt = null;
			Class.forName(classForName);
			String jdbcUrl = "jdbc:postgresql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName
					+ "&password=" + password;
			Properties properties = new Properties();
			properties.put("sslfactory", "org.postgresql.ssl.LibPQFactory");
			properties.put("sslrootcert","cert.crt");
			properties.put("sslmode", "verify-full");
			properties.put("ssl", "true");

			Connection con = DriverManager.getConnection(jdbcUrl, properties);
			System.out.println("Remote connection successful --> getRemoteConnectionWithSSLLibPQFactory");
			stmt = null != con ? con.createStatement() : null;
			if (null != stmt) {
				ResultSet rs = stmt.executeQuery(query);
				System.out.println("table data fetched");
				while (rs.next()) {
					System.out.println("data :" + rs.getString("data") + "   " + "data1 :" + rs.getString("data_one"));
				}
			}
			stmt.close();
		} catch (ClassNotFoundException e) {
			System.out.println(e.toString());
		} catch (SQLException e) {
			System.out.println(e.toString());
		}
	}
	
	private static void getRemoteConnectionWithSSLSingleCertificateFactoryWithFile()
			throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {

		try {

			Statement stmt = null;
			Class.forName(classForName);
			String jdbcUrl = "jdbc:postgresql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName
					+ "&password=" + password;
			Properties properties = new Properties();
			properties.put("sslmode", "verify-full");
			properties.put("ssl", "true");
			properties.put("sslfactory", "org.postgresql.ssl.SingleCertValidatingFactory");
            properties.put("sslfactoryarg", "file:cert.crt");

			Connection con = DriverManager.getConnection(jdbcUrl, properties);
			System.out.println("Remote connection successful --> getRemoteConnectionWithSSLSingleCertificateFactoryWithFile");
			stmt = null != con ? con.createStatement() : null;
			if (null != stmt) {
				ResultSet rs = stmt.executeQuery(query);
				System.out.println("table data fetched");
				while (rs.next()) {
					System.out.println("data :" + rs.getString("data") + "   " + "data1 :" + rs.getString("data_one"));
				}
			}
			stmt.close();
		} catch (ClassNotFoundException e) {
			System.out.println(e.toString());
		} catch (SQLException e) {
			System.out.println(e.toString());
		}
	}
	
	/*
	 *  https://github.com/pgjdbc/pgjdbc/issues/1293
	 */
	private static void getRemoteConnectionWithSSLSingleCertificateFactoryWithKeyStore()
			throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {

		try {

			 KeyStore sr = KeyStore.getInstance("PKCS12"); 
			  
	            // keystore password is required to access keystore 
	            char[] pass = ("Password").toCharArray(); 
	  
	            // creating and intializing object of InputStream 
	            InputStream is 
	                = new FileInputStream( 
	                    "keystore.p12"); 
	  
	            // intializing keystore object 
	            sr.load(is, pass); 
	  
	            // getting the certificate 
	            // using getCertificate() method 
	            //Pass the alias name of the rds certificate
	            X509Certificate cert 
	                = (X509Certificate)sr 
	                      .getCertificate("rds"); 
			
	            String encodedCertText = BaseEncoding.base64()
                     .withSeparator(LINE_SEPARATOR, LINE_LENGTH)
                     .encode(cert.getEncoded());
	            
			Statement stmt = null;
			Class.forName(classForName);
			String jdbcUrl = "jdbc:postgresql://" + hostname + ":" + port + "/" + dbName + "?user=" + userName
					+ "&password=" + password;
			Properties properties = new Properties();
			properties.put("sslmode", "verify-full");
			properties.put("ssl", "true");
			properties.put("sslfactory", "org.postgresql.ssl.SingleCertValidatingFactory");
			properties.put("sslfactoryarg",BEGIN_CERT + encodedCertText + END_CERT);
			
			Connection con = DriverManager.getConnection(jdbcUrl, properties);
			System.out.println("Remote connection successful --> getRemoteConnectionWithSSLSingleCertificateFactoryWithKeyStore");
			stmt = null != con ? con.createStatement() : null;
			if (null != stmt) {
				ResultSet rs = stmt.executeQuery(query);
				System.out.println("table data fetched");
				while (rs.next()) {
					System.out.println("data :" + rs.getString("data") + "   " + "data1 :" + rs.getString("data_one"));
				}
			}
			stmt.close();
		} catch (ClassNotFoundException e) {
			System.out.println(e.toString());
		} catch (SQLException e) {
			System.out.println(e.toString());
		}
	}

	@SuppressWarnings("resource")
	public static void main(String args[]) throws SQLException {
		try {
			getRemoteConnectionWithoutSSL();
			getRemoteConnectionWithSSLLibPQFactory();
			getRemoteConnectionWithSSLSingleCertificateFactoryWithFile();
			getRemoteConnectionWithSSLSingleCertificateFactoryWithKeyStore();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
