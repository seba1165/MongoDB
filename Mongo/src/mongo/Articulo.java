/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mongo;

/**
 *
 * @author Seba
 */
public class Articulo {
    String title;
    String text;

    public void setText(String text) {
        this.text = text;
    }
     
   public String getTitle() {  
      return title;  
   }  
     
   public void setTitle(String title) {  
      this.title = title;  
   }  
  
   @Override  
   public String toString() {  
      StringBuilder sb = new StringBuilder();  
      sb.append("\nTitle: "+title);
      sb.append("\nText: "+text);
      return sb.toString();  
   }

    void agregaTexto(String valor) {
        text = text + valor;
    }  
}