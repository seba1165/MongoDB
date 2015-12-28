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
        String collectionName1 = "Wikipedia";
        DBCollection Wikipedia;
        DBCollection IndiceInvertido;
        if (db.collectionExists(collectionName1) == false) {
            //I can confirm that the collection is created at this point.
            Wikipedia = db.createCollection(collectionName1, new BasicDBObject());
            //I would highly recommend you check the 'school' DBCollection to confirm it was actually created
            System.out.println("Collection Wikipedia creada con exito");
        } else {
            Wikipedia = db.getCollection(collectionName1);
        }

        String collectionName2 = "IndiceInvertido";

        if (db.collectionExists(collectionName2) == false) {
            //I can confirm that the collection is created at this point.
            IndiceInvertido = db.createCollection(collectionName2, new BasicDBObject());
            //I would highly recommend you check the 'school' DBCollection to confirm it was actually created
            System.out.println("Collection IndiceInvertido creada con exito");
        } else {
            IndiceInvertido = db.getCollection(collectionName2);
        }
        BasicDBObject indexWiki = new BasicDBObject("title", 1);
        BasicDBObject indexIndice = new BasicDBObject("key", 1);

        Wikipedia.createIndex(indexWiki);
        IndiceInvertido.createIndex(indexIndice);

        try {
            Articulo articulo = new Articulo();
            // Creamos la factoria de parseadores por defecto  
            XMLReader reader = XMLReaderFactory.createXMLReader();
            // Añadimos nuestro manejador al reader pasandole el objeto articulo  
            reader.setContentHandler(new LectorXML(articulo, Stopwords));
            reader.parse(new InputSource(new FileInputStream("eswiki-20151202-pages-meta-current1.xml")));
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        menu(db, Stopwords);
        //db.getCollection("Wikipedia").drop();
        //db.getCollection("IndiceInvertido").drop();
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

    private static void menu(DB db, ArrayList Stopwords) {
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
                    insertarArticulo(title, text, db, Stopwords);
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
                        filtraPalabras(IndiceInvertido, dbObj, Stopwords);
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

    public static void insertarArticulo(String title, String text, DB db, ArrayList Stopwords) {
        DBCollection Wikipedia = db.getCollection("Wikipedia");
        DBCollection IndiceInvertido = db.getCollection("IndiceInvertido");

        BasicDBObject artWiki = new BasicDBObject();

        artWiki.put("title", title);
        //artWiki.put("text", text);

        if (Wikipedia.findOne(artWiki) == null) {
            artWiki.put("text", text);
            Wikipedia.insert(artWiki);
            filtraPalabras(IndiceInvertido, Wikipedia.findOne(artWiki), Stopwords);
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

    private static void filtraPalabras(DBCollection IndiceInvertido, DBObject artWiki, ArrayList Stopwords) {

        String title = (String) artWiki.get("title");
        String text = (String) artWiki.get("text");
        List<String> list = new ArrayList<String>();

        String delimitadores = "[ -<>/.=,;:?!¡¿\\r?\\n|\\}\\{\'\"\\[\\]]+";
        String[] palabrasSeparadas = text.split(delimitadores);
        System.out.println(palabrasSeparadas.length);
        
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
            insertarEnIndiceInvertido(key, Collections.frequency(list, key), artWiki, IndiceInvertido);
        }
    }

    private static void insertarEnIndiceInvertido(String key, int frequency, DBObject artWiki, DBCollection IndiceInvertido) {
        BasicDBObject palabra = new BasicDBObject();
        palabra.put("key", key);
        DBObject palabraEnIndice = IndiceInvertido.findOne(palabra);

        BasicDBObject articulo = new BasicDBObject();
        articulo.put("idArt", artWiki.get("_id"));
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
}
