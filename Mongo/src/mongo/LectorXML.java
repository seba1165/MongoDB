/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongo;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import java.util.ArrayList;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author Seba
 */
public class LectorXML extends DefaultHandler{
    
    private String valor = null;  
    private Articulo articulo;
    MongoClient mongoClient; 
    DB db;
    ArrayList Stopwords;
    int particiones;
     
    public LectorXML(Articulo articulo, ArrayList Stopwords, int particiones){  
       this.articulo=articulo;
       mongoClient = new MongoClient();
       db = mongoClient.getDB("LabSD");
       this.Stopwords = Stopwords;
       this.particiones = particiones;
    }  

    @Override  
    public void startElement(String uri, String localName, String name,  Attributes attributes) throws SAXException {  

       // Limpiamos la variable temporal.  
       valor=null;   
    }  

    @Override  
    public void characters(char[] ch, int start, int length) throws SAXException {  
       // Guardamos el texto en la variable temporal  
       valor = new String(ch,start,length);
       articulo.agregaTexto(valor);
    }  

    @Override  
    public void endElement(String uri, String localName, String name) throws SAXException {  
       // Según la etiqueta guardamos el valor leido   
       // en una propiedad del objeto libro  
        if (localName.equals("title")){  
            articulo.setTitle(valor);  
        }else if (localName.equals("siteinfo")){
            articulo.setText("");
        }else if (localName.equals("format")) {
            articulo.setText("");
        }else if (localName.equals("text")){
            Mongo.insertarArticulo(articulo.title, articulo.text, db, Stopwords, particiones);
            articulo.setText("");
       }
    }  
}
