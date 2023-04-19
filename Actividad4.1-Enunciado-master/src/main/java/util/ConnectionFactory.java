package util;

import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.Properties;

import org.neodatis.odb.ODB;
import org.neodatis.odb.ODBFactory;

public class ConnectionFactory {
	private static ODB connection = null;
	
//Clase Properties de Java
//leer un fichero .properties con pares clave valor.
private static final String DB_CONFIG_FILE = Paths.get("src/main/resources", "db.properties").toString();
//Si nos hacen falta algunos mas??
//Determinar el driver, urll user, etc
//Las claves del fichero Properties:
private static final String DB_FILE = "db.file";

	private ConnectionFactory() {
	}
	
	//https://docs.oracle.com/javase/8/docs/api/java/util/Properties.html
	   
	//utilizando el patrón Singleton cree un objeto org.neodatis.odb.ODB que lea 
	//el fichero db.properties y obtenga la conexión a la BDOO.
	static {
		
		try {
			//TODO
			 Properties props = new Properties();
			 FileInputStream fis = new FileInputStream(DB_CONFIG_FILE);
	         props.load(fis);
	         String dbFile = props.getProperty(DB_FILE);
	         connection = ODBFactory.open(dbFile);			
			
		} catch (Exception ex) {
			//TODO: revisar que excepciones lanza
			ex.printStackTrace();
		}
	}
	

	public static ODB getConnection() {		
		return connection;
	}

	//Añade un método estático para cerrar la conexion
	public static void closeConnection() {
		if (!(connection == null)) {
			connection.close();
		}
	}

}
