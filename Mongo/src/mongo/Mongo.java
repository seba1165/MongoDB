/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import org.bson.types.ObjectId;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 *
 * @author Seba
 */
public class Mongo {

    public static void main(String[] args) throws FileNotFoundException, IOException, SAXException {
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("LabSD");
        ArrayList Stopwords = leerStopWords();
        String [] Part;
        File archivo = new File("Config.txt");
        FileReader fr = new FileReader(archivo);
        BufferedReader br = new BufferedReader(fr);
        String linea1 = br.readLine();
        Part = linea1.split(" ");
        int canTpart = Integer.parseInt(Part[1]);
        fr.close();
        DBCollection Wikipedia[] = new DBCollection[canTpart];
        DBCollection IndiceInvertido[] = new DBCollection[canTpart];
        BasicDBObject indexWiki = new BasicDBObject("title", 1);
        BasicDBObject indexIndice = new BasicDBObject("key", 1);
        if(canTpart<1){
            System.out.println("Ingrese los parametros de forma correcta");
        }else{
            for (int i = 0; i < canTpart; i++) {
                String collectionWiki = "Wikipedia";
                String WikiNum = collectionWiki + i;
                if (db.collectionExists(WikiNum) == false) {
                Wikipedia[i] = db.createCollection(WikiNum, new BasicDBObject());

                } else {
                    Wikipedia[i] = db.getCollection(WikiNum);
                }
                Wikipedia[i].createIndex(indexWiki);
            }
            System.out.println("Collections de Wikipedia creadas con exito");
            for (int i = 0; i < canTpart; i++) {
                String collectionIndice = "IndiceInvertido";
                String IndiceNum = collectionIndice + i;
                if (db.collectionExists(IndiceNum) == false) {
                IndiceInvertido[i] = db.createCollection(IndiceNum, new BasicDBObject());
                } else {
                    IndiceInvertido[i] = db.getCollection(IndiceNum);
                }
                IndiceInvertido[i].createIndex(indexIndice);
            }
            System.out.println("Collections de IndiceInvertido creadas con exito");

            try {
                Articulo articulo = new Articulo();
                // Creamos la factoria de parseadores por defecto  
                XMLReader reader = XMLReaderFactory.createXMLReader();
                // Añadimos nuestro manejador al reader pasandole el objeto articulo  
                reader.setContentHandler(new LectorXML(articulo, Stopwords, canTpart));
                reader.parse(new InputSource(new FileInputStream("eswiki-20151202-pages-meta-current1.xml")));
            } catch (SAXException | IOException e) {
                e.printStackTrace();
            }
            menu(db, Stopwords, canTpart);
            //dropCollections(Wikipedia, IndiceInvertido, db);
        }
    }

    private static ArrayList leerStopWords() {
        ArrayList Stopwords = new ArrayList();
        FileReader fr = null;
        BufferedReader br = null;
        //Cadena de texto donde se guardara el contenido del archivo
        String contenido = "";
        try {
            //ruta puede ser de tipo String o tipo File
            //si usamos un File debemos hacer un import de esta clase
            //File ruta = new File( "/home/usuario/texto.txt" );
            String ruta = "Stopwords.txt";
            fr = new FileReader(ruta);
            br = new BufferedReader(fr);

            String linea;
            //Obtenemos el contenido del archivo linea por linea
            while ((linea = br.readLine()) != null) {
                Stopwords.add(linea);
            }

        } catch (Exception e) {
        } //finally se utiliza para que si todo ocurre correctamente o si ocurre
        //algun error se cierre el archivo que anteriormente abrimos
        finally {
            try {
                br.close();
            } catch (Exception ex) {
            }
        }
        return Stopwords;
    }
    
    private static void menu(DB db, ArrayList Stopwords, int particiones) {
        DBCollection Wikipedia = db.getCollection("Wikipedia");
        DBCollection IndiceInvertido = db.getCollection("IndiceInvertido");
        Scanner scanner = new Scanner(System.in); //Sirve para recoger texto por consola
        int select = -1; //opción elegida del usuario
        while (select != 0) {

            System.out.println("Elige opción:\n1.- Insertar Articulo"
                    + "\n2.- Modificar Articulo\n"
                    + "3.- Eliminar Articulo\n"
                    + "0.- Salir");
            //Recoger una variable por consola
            select = Integer.parseInt(scanner.nextLine());

            //Ejemplo de switch case en Java
            switch (select) {
                case 1:
                    scanner.reset();
                    System.out.print("Ingrese el titulo del articulo: ");
                    String title = scanner.nextLine();
                    System.out.println("Escriba el artículo a continuación:");
                    scanner.reset();
                    String text = scanner.nextLine();
//                    insertarArticulo(title, text, db, Stopwords, select)
//                    insertarArticulo(title, text, db, Stopwords);
                    break;
                case 2:
                    scanner.reset();
                    System.out.print("Ingrese el id del articulo a buscar para la modificación: ");
                    String idleido = scanner.nextLine();
                    idleido = "5680685723ce41360064c9a4";
                    DBObject dbObj = findDocumentById(idleido, db);
                    if (dbObj != null) {
                        System.out.println("Articulo encontrado");
                        scanner.reset();
                        borraPalabrasIndice(IndiceInvertido, dbObj);
                        System.out.print("Ingrese el nuevo titulo del articulo: ");
                        title = scanner.nextLine();
                        System.out.println("Escriba el artículo a continuación:");
                        scanner.reset();
                        text = scanner.nextLine();
                        BasicDBObject articuloModificado = new BasicDBObject();
                        articuloModificado.put("title", title);
                        articuloModificado.put("text", text);
                        Wikipedia.update(dbObj, articuloModificado);
                        dbObj = findDocumentById(idleido, db);
                        filtraPalabras(dbObj, Stopwords, particiones, db);
                    } else {
                        System.out.println("Articulo no encontrado");
                    }

                    break;
                case 3:
                    scanner.reset();
                    System.out.print("Ingrese el id del articulo a buscar para la eliminacion: ");
                    idleido = scanner.nextLine();
                    idleido = "5680685723ce41360064c9a4";
                    DBObject dbObjel = findDocumentById(idleido, db);
                    if (dbObjel != null) {
                        System.out.println("Articulo encontrado");
                        borraPalabrasIndice(IndiceInvertido, dbObjel);
                        Wikipedia.remove(dbObjel);
                    } else {
                        System.out.println("Articulo no encontrado");
                    }
                    break;
                case 0:
                    System.out.println("Adios!");
                    break;
                default:
                    System.out.println("Número no reconocido");
                    break;
            }

        }
    }

    public static void insertarArticulo(String title, String text, DB db, ArrayList Stopwords, int particiones) {
//        DBCollection Wikipedia = db.getCollection("Wikipedia");
//        DBCollection IndiceInvertido = db.getCollection("IndiceInvertido");
        int intColeccion = funcion_hash(title, particiones);
        String coleccionWiki = "Wikipedia"+intColeccion;
        DBCollection Wikipedia = db.getCollection(coleccionWiki);
        BasicDBObject artWiki = new BasicDBObject();
         

        artWiki.put("title", title);
        //artWiki.put("text", text);

        if (Wikipedia.findOne(artWiki) == null) {
            artWiki.put("text", text);
            Wikipedia.insert(artWiki);
            filtraPalabras(Wikipedia.findOne(artWiki), Stopwords, particiones, db);
        } else {
            //System.out.println("Lo sentimos, el articulo no se puede insertar debido a que ya existe");
        }
    }

    private static void borraPalabrasIndice(DBCollection IndiceInvertido, DBObject dbObj) {
        BasicDBObject query = new BasicDBObject("articulos.idArt", dbObj.get("_id"));
        DBCursor cursor = IndiceInvertido.find(query);
        try {
            while (cursor.hasNext()) {
                DBObject cur = cursor.next();
                System.out.println(cur.get("_id"));
                BasicDBObject update2 = new BasicDBObject("articulos", new BasicDBObject("idArt", dbObj.get("_id")));
                IndiceInvertido.update(cur, new BasicDBObject("$pull", update2));
            }
        } finally {
            cursor.close();
        }
    }

    private static void filtraPalabras(DBObject artWiki, ArrayList Stopwords, int particiones, DB db) {

        String title = (String) artWiki.get("title");
        String text = (String) artWiki.get("text");
        List<String> list = new ArrayList<String>();

        String delimitadores = "[ -<>/.=,;:?!¡¿\\r?\\n|\\}\\{\'\"\\[\\]]+";
        String[] palabrasSeparadas = text.split(delimitadores);
        //System.out.println(palabrasSeparadas.length);
        
        for (int i = 0; i < palabrasSeparadas.length; i++) {
            if(palabrasSeparadas[i].length()!=0 && palabrasSeparadas[i].length()!=1 && !palabrasSeparadas[i].equals("br")){
                if(!Stopwords.contains(palabrasSeparadas[i].toLowerCase())){
                    //System.out.println(palabrasSeparadas[i]);
                    list.add(palabrasSeparadas[i].toLowerCase());
                }
            }
        }
        Set<String> quipu = new HashSet<String>(list);
        for (String key : quipu) {
            int particion = funcion_hash(key, particiones);
            String coleccionIndice = "IndiceInvertido"+particion;
            DBCollection IndiceInvertido = db.getCollection(coleccionIndice);
            insertarEnIndiceInvertido(key, Collections.frequency(list, key), artWiki, IndiceInvertido);
        }
    }

    private static void insertarEnIndiceInvertido(String key, int frequency, DBObject artWiki, DBCollection IndiceInvertido) {
        BasicDBObject palabra = new BasicDBObject();
        palabra.put("key", key);
        //System.out.println(palabra);
        DBObject palabraEnIndice = IndiceInvertido.findOne(palabra);
        //System.out.println(palabra);
        BasicDBObject articulo = new BasicDBObject();
        articulo.put("idArt", artWiki.get("_id"));
        articulo.put("title", artWiki.get("title"));
        articulo.put("frecuencia", frequency);

        if (palabraEnIndice == null) {
            List<BasicDBObject> articulos = new ArrayList<>();
            articulos.add(articulo);
            BasicDBObject palabraAInsertar = new BasicDBObject();
            palabraAInsertar.put("key", key);
            palabraAInsertar.put("articulos", articulos);
            IndiceInvertido.insert(palabraAInsertar); 
        } else {
            IndiceInvertido.update(new BasicDBObject("key", key),
                    new BasicDBObject("$addToSet", new BasicDBObject("articulos", articulo)));
        }
    }

    public static DBObject findDocumentById(String id, DB db) {
        DBCollection Wikipedia = db.getCollection("Wikipedia");
        BasicDBObject query = new BasicDBObject();
        query.put("_id", new ObjectId(id));
        DBObject dbObj = Wikipedia.findOne(query);
        return dbObj;
    }

    private static void dropCollections(DBCollection[] Wikipedia, DBCollection[] IndiceInvertido, DB db) {
        for (int i = 0; i < Wikipedia.length; i++) {
            Wikipedia[i].drop();
            IndiceInvertido[i].drop();
        }
    }
    
    private static int funcion_hash(String x, int particiones) {
        char ch[];
        ch = x.toCharArray();
        int xlength = x.length();
        int i, sum;
        for (sum=0, i=0; i < x.length(); i++)
            sum += ch[i];
        return sum % particiones;
    }
}
