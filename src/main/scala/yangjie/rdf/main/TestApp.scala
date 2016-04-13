package yangjie.rdf.main

import org.apache.jena.query.{Query, QueryFactory}
import org.apache.jena.sparql.algebra.{Algebra, Op}
import org.apache.jena.sparql.sse.SSE
import org.apache.jena.sparql.syntax.{ElementPathBlock, ElementVisitorBase, ElementWalker}


/**
  * Created by yangjiecloud on 2016/4/5.
  */
object TestApp {
  def main(args: Array[String]): Unit = {
    val query = QueryFactory.create("select ?X ?Y ?Z {?X <http://www.w3.org/2001/vcard-rdf/3.0#FN> ?Z . ?Z <http://www.w3.org/2001/vcard-rdf/3.0#FN> ?Y . ?X <http://www.w3.org/2001/vcard-rdf/3.0#FN> ?Y . ?X <http://www.w3.org/2001/vcard-rdf/3.0#FN> 'Grad' . ?Y <http://www.w3.org/2001/vcard-rdf/3.0#dd> 'Univ' . ?Z<http://www.w3.org/2001/vcard-rdf/3.0#xx> 'Depart'}") ;
    val pattern = query.getQueryPattern()
    ElementWalker.walk(pattern,new ElementVisitorBase() {
      override def visit(el:ElementPathBlock): Unit = {
        val triples = el.patternElts()
        while (triples.hasNext) {
          println(triples.next())
        }
      }
    })
    val op = Algebra.compile(pattern) ;
    println(op.getClass)
  }
}
