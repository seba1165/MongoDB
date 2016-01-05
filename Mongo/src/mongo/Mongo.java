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
        //Cliente Mongo para acceder a la BD
        MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("LabSD");
        //Se leen las stopwords para filtrar el ingreso de datos al indice invertido
        ArrayList Stopwords = leerStopWords();
        String [] Part;
        //Se lee el archivo de configuracion, donde la linea corresponde a la cantidad 
        //de particiones de la bd
        File archivo = new File("Config.txt");
        FileReader fr = new FileReader(archivo);
        BufferedReader br = new BufferedReader(fr);
        String linea1 = br.readLine();
        Part = linea1.split(" ");
        int canTpart = Integer.parseInt(Part[1]);
        fr.close();
        //Se crean las collections y los indices de la bd
        DBCollection Wikipedia[] = new DBCollection[canTpart];
        DBCollection IndiceInvertido[] = new DBCollection[canTpart];
        BasicDBObject indexWiki = new BasicDBObject("title", 1);
        BasicDBObject indexIndice = new BasicDBObject("key", 1);
        //Si la cantidad de particiones en el config es menor a 1, el programa no se inicia
        if(canTpart<1){
            System.out.println("Ingrese los parametros de forma correcta");
        }else{
            //Si existe una bd, se verifica si es de la misma cantidad de particiones
            //Si es asi, se conserva la bd existente
            //Si no, se borra y se crea una nueva
            if((db.getCollectionNames().size()/2)!=canTpart){
                //Se borran las collections
                dropCollections(Wikipedia, IndiceInvertido, db);
                for (int i = 0; i < canTpart; i++) {
                    //Las collections se nombraran como "Wikipedia0" hasta "Wikipedia(N-1)", siendo
                    //N la cantidad de particiones ingresadas en el config
                    String collectionWiki = "Wikipedia";
                    String WikiNum = collectionWiki + i;
                    //Se crea la collection "Wikipediai"
                    Wikipedia[i] = db.createCollection(WikiNum, new BasicDBObject());
                    //Se crea el indice por titulo para los articulos
                    Wikipedia[i].createIndex(indexWiki);
                }
                System.out.println("Collections de Wikipedia creadas con exito");
                
                for (int i = 0; i < canTpart; i++) {
                    //Las collections se nombraran como "IndiceInvertido0" hasta "IndiceInvertido0(N-1)", siendo
                    //N la cantidad de particiones ingresadas en el config
                    //El procedimiento es el mismo que para los articulos
                    String collectionIndice = "IndiceInvertido";
                    String IndiceNum = collectionIndice + i;
                    //Se crea la collection "IndiceInvertidoi"
                    IndiceInvertido[i] = db.createCollection(IndiceNum, new BasicDBObject());
                    //Se crea el indice por palabra
                    IndiceInvertido[i].createIndex(indexIndice);
                }
                System.out.println("Collections de IndiceInvertido creadas con exito");
            //Si ya existen las collections
            }else{
                //Solo se obtienen
                for (int i = 0; i < canTpart; i++) {
                    String collectionWiki = "Wikipedia";
                    String WikiNum = collectionWiki + i;
                    Wikipedia[i] = db.getCollection(WikiNum);
                }
                for (int i = 0; i < canTpart; i++) {
                    String collectionIndice = "IndiceInvertido";
                    String IndiceNum = collectionIndice + i;
                    IndiceInvertido[i] = db.getCollection(IndiceNum);
                } 
            }

            //Se lee el archivo de entrada para llenar la bd
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

    //Función para leer las stopwords y pasarlas a un ArrayList y luego retornar dicho ArrayList
    private static ArrayList leerStopWords() {
        ArrayList Stopwords = new ArrayList();
        FileReader fr = null;
        BufferedReader br = null;
        //Cadena de texto donde se guardara el contenido del archivo
        String contenido = "";
        try {
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
    
    //Funcion para mostrar un menu luego de insertar todos los articulos del archivo de entrada
    private static void menu(DB db, ArrayList Stopwords, int particiones) {
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
            switch (select) {
                case 1:
                    //Para insertar un articulo solo se ingresa el titulo y el texto
                    scanner.reset();
                    System.out.print("Ingrese el titulo del articulo: ");
                    String title = scanner.nextLine();
                    System.out.println("Escriba el artículo a continuación:");
                    scanner.reset();
                    String text = scanner.nextLine();
                    insertarArticulo(title, text, db, Stopwords, particiones);
                    break;
                case 2:
                    //Para modificar, se ingresa el titulo del articulo que se desea modificar, para luego buscarlo
                    scanner.reset();
                    System.out.print("Ingrese el titulo del articulo a buscar para la modificación: ");
                    title = scanner.nextLine();
                    //Se calcula que particion de la bd de articulos deberia ocupar el articulo buscado
                    int intColeccion = funcion_hash(title, particiones);
                    System.out.println("Wikipedia"+intColeccion);
                    String coleccionWiki = "Wikipedia"+intColeccion;
                    DBCollection Wikipedia = db.getCollection(coleccionWiki);
                    BasicDBObject query = new BasicDBObject("title", title);
                    DBObject dbObj = Wikipedia.findOne(query);
                    //Si el articulo esta en la bd
                    if (dbObj != null) {
                        System.out.println("Articulo encontrado");
                        scanner.reset();
                        //Se borran las palabras del texto del articulo, del indice invertido
                        borraPalabrasIndice(dbObj, particiones, db);
                        System.out.println("Escriba el artículo a continuación:");
                        scanner.reset();
                        text = scanner.nextLine();
                        BasicDBObject articuloModificado = new BasicDBObject();
                        articuloModificado.put("text", text);
                        //Se modifica el texto del archivo encontrado, en la bd
                        Wikipedia.update(dbObj, articuloModificado);
                        dbObj = Wikipedia.findOne(query);
                        //Se ingresan las nuevas palabras del texto a la bd del indice invertido
                        filtraPalabras(dbObj, Stopwords, particiones, db);
                    } else {
                        System.out.println("Articulo no encontrado");
                    }

                    break;
                case 3:
                    //Similar a la modificacion de articulo
                    scanner.reset();
                    System.out.print("Ingrese el titulo del articulo a buscar para la modificación: ");
                    title = scanner.nextLine();
                    intColeccion = funcion_hash(title, particiones);
                    coleccionWiki = "Wikipedia"+intColeccion;
                    Wikipedia = db.getCollection(coleccionWiki);
                    query = new BasicDBObject("title", title);
                    DBObject dbObjel = Wikipedia.findOne(query);
                    if (dbObjel != null) {
                        System.out.println("Articulo encontrado");
                        //Si se encuentra el articulo, se borran las palabras de su texto del indice invertido
                        borraPalabrasIndice(dbObjel, particiones, db);
                        //Se borra el articulo de la bd Wikipedia
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

    //Funcion para insertar un articulo a la particion de la bd Wikipedia correspondiente
    public static void insertarArticulo(String title, String text, DB db, ArrayList Stopwords, int particiones) {
        //Se calcula en que particion de la bd Wikipedia debe quedar el articulo
        int intColeccion = funcion_hash(title, particiones);
        String coleccionWiki = "Wikipedia"+intColeccion;
        DBCollection Wikipedia = db.getCollection(coleccionWiki);
        BasicDBObject artWiki = new BasicDBObject();
        artWiki.put("title", title);
        
        //Se verifica si el articulo a insertar ya existe en la bd
        if (Wikipedia.findOne(artWiki) == null) {
            artWiki.put("text", text);
            Wikipedia.insert(artWiki);
            //Si no existe, se ingresa y se procede a filtrar las palabras del texto para ingresarlas
            //al indice invertido
            filtraPalabras(Wikipedia.findOne(artWiki), Stopwords, particiones, db);
        } else {
            //System.out.println("Lo sentimos, el articulo no se puede insertar debido a que ya existe");
        }
    }

    //Cuando se elimina o modifica un articulo se debe borras las palabras del indice invertido correspondientes
    private static void borraPalabrasIndice(DBObject dbObj, int particiones, DB db) {
        //Se define el id del articulo a buscar dentro del arreglo de articulos perteneciente a la palabra del indice invertido
        BasicDBObject query = new BasicDBObject("articulos.idArt", dbObj.get("_id"));
        DBCollection IndiceInvertido;
        //Se busca en todas las particiones
        for (int i = 0; i < particiones; i++) {
            String collectionIndice = "IndiceInvertido";
            String IndiceNum = collectionIndice + i;
            IndiceInvertido = db.getCollection(IndiceNum);
            //cursor obtendra todas las palabras que dentro de su arreglo de articulos contenga al articulo buscado
            DBCursor cursor = IndiceInvertido.find(query);
            try {
                while (cursor.hasNext()) {
                    DBObject cur = cursor.next();
                    //System.out.println(cur.get("_id"));
                    BasicDBObject update2 = new BasicDBObject("articulos", new BasicDBObject("idArt", dbObj.get("_id")));
                    //Se saca el articulo del arreglo
                    IndiceInvertido.update(cur, new BasicDBObject("$pull", update2));
                }
            } finally {
                cursor.close();
            }
        }
    }

    //Método para filtrar las palabras del texto de un articulo y luego insertarlas en el indice invertido correspondiente
    private static void filtraPalabras(DBObject artWiki, ArrayList Stopwords, int particiones, DB db) {
        String title = (String) artWiki.get("title");
        String text = (String) artWiki.get("text");
        List<String> list = new ArrayList<String>();
        //Elementos inservibles del texto
        String delimitadores = "[ -<>/.=,;:?!¡¿\\r?\\n|\\}\\{\'\"\\[\\]]+";
        //Palabras del texto, separadas
        String[] palabrasSeparadas = text.split(delimitadores);
        //System.out.println(palabrasSeparadas.length);
        
        for (int i = 0; i < palabrasSeparadas.length; i++) {
            //Si se paso algun otro simbolo o si la palabra no sirve para el indice invertido
            if(palabrasSeparadas[i].length()!=0 && palabrasSeparadas[i].length()!=1 && !palabrasSeparadas[i].equals("br")){
                //Si la palabra no es stopword
                if(!Stopwords.contains(palabrasSeparadas[i].toLowerCase())){
                    //Se agrega a un arreglo dinamico
                    list.add(palabrasSeparadas[i].toLowerCase());
                }
            }
        }
        //
        Set<String> quipu = new HashSet<String>(list);
        //Para cada palabra dentro del arreglo dinamico
        for (String key : quipu) {
            //Se calcula en que particion del indice invertido va la palabra
            int particion = funcion_hash(key, particiones);
            String coleccionIndice = "IndiceInvertido"+particion;
            //Se obtiene la collection
            DBCollection IndiceInvertido = db.getCollection(coleccionIndice);
            //Se inserta a la collection, indicando cuantas veces se repite dicha palabra
            insertarEnIndiceInvertido(key, Collections.frequency(list, key), artWiki, IndiceInvertido);
        }
    }

    //Método para insertar un articulo en el arreglo de articulos de una palabra o para crear la palabra en el indice invertido
    //si es que no existe
    private static void insertarEnIndiceInvertido(String key, int frequency, DBObject artWiki, DBCollection IndiceInvertido) {
        BasicDBObject palabra = new BasicDBObject();
        palabra.put("key", key);
        //Se busca la palabra en la particion de indice invertido correspondiente
        DBObject palabraEnIndice = IndiceInvertido.findOne(palabra);
        //Se crea el articulo, con su id, titulo y frecuencia de la palabra en dicho articulo
        BasicDBObject articulo = new BasicDBObject();
        articulo.put("idArt", artWiki.get("_id"));
        articulo.put("title", artWiki.get("title"));
        articulo.put("frecuencia", frequency);
        //Si no existe la palabra, se crea y se inserta el articulo
        if (palabraEnIndice == null) {
            List<BasicDBObject> articulos = new ArrayList<>();
            articulos.add(articulo);
            BasicDBObject palabraAInsertar = new BasicDBObject();
            palabraAInsertar.put("key", key);
            palabraAInsertar.put("articulos", articulos);
            IndiceInvertido.insert(palabraAInsertar); 
        //Si existe la palabra, se actualiza el arreglo de articulos
        } else {
            IndiceInvertido.update(new BasicDBObject("key", key),
                    new BasicDBObject("$addToSet", new BasicDBObject("articulos", articulo)));
        }
    }

    //Método para borrar las collections 
    private static void dropCollections(DBCollection[] Wikipedia, DBCollection[] IndiceInvertido, DB db) {
        for (int i = 0; i < Wikipedia.length; i++) {
            Wikipedia[i].drop();
            IndiceInvertido[i].drop();
        }
    }
    
    //Método para calcular la particion para un articulo, con su titulo o para calcular la particion de una palabra en el indice invertido
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
