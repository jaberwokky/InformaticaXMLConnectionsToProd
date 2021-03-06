/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import scala.xml._
import scala.xml.transform._
import scala.xml.Elem
import java.io._

object InformaticaXMLConnectionsToProd {

  /**
   * @param args the command line arguments
   */
  def main(args: Array[String]): Unit = {

    val xml = scala.xml.XML.load(args(0))

    case class GenAttr(
      pre: Option[String],
      key: String,
      value: Seq[Node],
      next: MetaData
    ) {
      def toMetaData = Attribute(pre, key, value, next)
    }

    def decomposeMetaData(m: MetaData): Option[GenAttr] = m match {
      case Null => None
      case PrefixedAttribute(pre, key, value, next) =>
        Some(GenAttr(Some(pre), key, value, next))
      case UnprefixedAttribute(key, value, next) =>
        Some(GenAttr(None, key, value, next))
    }

    def unchainMetaData(m: MetaData): Iterable[GenAttr] =
      m.flatMap(decomposeMetaData).toList.reverse

    def chainMetaData(l: Iterable[GenAttr]): MetaData = l match {
      case Nil => Null
      case head :: tail => head.copy(next = chainMetaData(tail)).toMetaData
    }

    def mapMetaData(m: MetaData)(f: GenAttr => GenAttr): MetaData =
      chainMetaData(unchainMetaData(m).map(f))

    val procs = ((xml \\ "SESSTRANSFORMATIONINST" \\ "@TRANSFORMATIONNAME")
      .zip(xml \\ "SESSTRANSFORMATIONINST" \\ "@TRANSFORMATIONTYPE"))
      .filter(_._2.text.contains("Stored"))

    val procsNames = procs.map(_._1)

    val procsNamesE = procsNames.map(_.text.split('.')(0).split('_').drop(1).reduce((x, y) => x + '_' + y))
    val procsNamesEO = procsNamesE.map(_.replace("_ORPHAN", ""))


    object SeestEditAttrs extends RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case e @ Elem(prefix, "ATTRIBUTE", attribs, scope, _*) =>
          Elem(prefix, "ATTRIBUTE", mapMetaData(e.attributes) {
            case g @ GenAttr(_, key, Text(v), _) if key == "VALUE" && !v.toString.contains("PROD") =>
              g.copy(value = Text(v + "_PROD"))
            case other => other
          }, TopScope)
        case other => other
      }
    }

    object RuleSesstEditAttrs extends RuleTransformer(SeestEditAttrs)

    object SeeseEditAttrs extends RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case e @ Elem(prefix, "CONNECTIONREFERENCE", attribs, scope, _*) =>
          Elem(prefix, "CONNECTIONREFERENCE", mapMetaData(e.attributes) {
            case g @ GenAttr(_, key, Text(v), _) if key == "CONNECTIONNAME" && !v.toString.contains("PROD") =>
              g.copy(value = Text(v + "_PROD"))
            case other => other
          }, TopScope)
        case other => other
      }
    }

    object RuleSesseEditAttrs extends RuleTransformer(SeeseEditAttrs)

    object SesstEdit extends RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case fn @ Elem(prefix, "ATTRIBUTE", attribs, scope, _*) if attribs.asAttrMap("NAME") == "Connection Information" => RuleSesstEditAttrs(fn)
        case other => other
      }
    }

    def addChild(n: Node, newChild: Node) = n match {
      case Elem(prefix, label, attribs, scope, child @ _*) =>
        Elem(prefix, label, attribs, scope, child ++ newChild: _*)
      case _ => sys.error("Can only add children to elements!")
    }

    class AddChildrenTo(newChild: Node) extends RewriteRule {
      override def transform(n: Node) = n match {
        case n @ Elem(_, "SESSTRANSFORMATIONINST", _, _, _*) => addChild(n, newChild)
        case other => other
      }
    }

    object RuleSesstEdit extends RuleTransformer(SesstEdit)

    object MainConnectionTransform extends RewriteRule {
      override def transform(n: Node): Seq[Node] = n match {
        case sesst @ Elem(_, "SESSTRANSFORMATIONINST", _, _, _*) if {
          val attrs = sesst.attributes.asAttrMap
          !attrs("TRANSFORMATIONTYPE").toLowerCase.contains("target") &&
            !attrs("TRANSFORMATIONNAME").toLowerCase.contains("dual") &&
            !procsNamesE.contains((attrs("TRANSFORMATIONNAME").split('.')(0).split('_').drop(1).reduceOption((x, y) => x + '_' + y)).getOrElse("null")) &&
            !procsNamesEO.contains((attrs("TRANSFORMATIONNAME").split('.').lift(1).toString.split('_').drop(1).reduceOption((x, y) => x + '_' + y)).getOrElse("null")) &&
            sesst.child.toString.toLowerCase.contains("connection information")
        } => RuleSesstEdit(sesst)
        case sesstl @ Elem(_, "SESSTRANSFORMATIONINST", _, _, _*) if {
          val attrs = sesstl.attributes.asAttrMap
          attrs("TRANSFORMATIONTYPE").toLowerCase.contains("lookup") &&
            !procsNamesE.contains((attrs("TRANSFORMATIONNAME").split('.')(0).split('_').drop(1).reduceOption((x, y) => x + '_' + y)).getOrElse("null")) &&
            !procsNamesEO.contains((attrs("TRANSFORMATIONNAME").split('.').lift(1).toString.split('_').drop(1).reduceOption((x, y) => x + '_' + y)).getOrElse("null")) &&
            !sesstl.child.toString.toLowerCase.contains("connection information")
        } => {
          val attrs = sesstl.attributes.asAttrMap
          val vl_prf = attrs("TRANSFORMATIONNAME").split('.').lift(1).toString.split('_').lift(1).toString
          val reg = """Some\((.*)\)""".r
          val reg2 = """None""".r
          val vl_p = vl_prf match {
            case reg(s1) => s1
            case _ => "ETL"
          }
          val vl = ("EDW_" + vl_p + "_PROD").toUpperCase
          addChild(n, <ATTRIBUTE NAME="Connection Information" VALUE={ vl }/>)
        }
        case sesse @ Elem(_, "SESSIONEXTENSION", _, _, _*) if {
          val attrs = sesse.attributes.asAttrMap
          !attrs("TRANSFORMATIONTYPE").toLowerCase.contains("target") &&
            !attrs("SINSTANCENAME").toLowerCase.contains("dual") &&
            !procsNamesE.contains((attrs("SINSTANCENAME").split('.')(0).split('_').drop(1).reduceOption((x, y) => x + '_' + y)).getOrElse("null")) &&
            !procsNamesEO.contains((attrs("SINSTANCENAME").split('.').lift(1).toString.split('_').drop(1).reduceOption((x, y) => x + '_' + y)).getOrElse("null"))
        } => RuleSesseEditAttrs(sesse)
        case other => other
      }
    }

    object RuleMainConnectionTransoform extends RuleTransformer(MainConnectionTransform)

    val newXml = RuleMainConnectionTransoform(xml)

    val ddd = new scala.xml.dtd.DocType(
      "POWERMART",
      scala.xml.dtd.SystemID("powrmart.dtd"),
      Nil
    )

    scala.xml.XML.save("testxmld.xml", newXml, "windows-1251", true, ddd)

    var listOfTables: List[String] = List()

    try {
      listOfTables = scala.io.Source.fromFile("listOfTablesToCopyFromProd").getLines.toList
    } catch {
      case e: Exception => {}
    }

    val procsLookup = ((xml \\ "SESSTRANSFORMATIONINST" \\ "@TRANSFORMATIONNAME")
      .zip(xml \\ "SESSTRANSFORMATIONINST" \\ "@TRANSFORMATIONTYPE"))
      .filter(_._2.text.contains("Lookup"))

    val procsLookupNames = procsLookup map (_._1)

    val procsStored = ((xml \\ "SESSTRANSFORMATIONINST" \\ "@TRANSFORMATIONNAME")
      .zip(xml \\ "SESSTRANSFORMATIONINST" \\ "@TRANSFORMATIONTYPE"))
      .filter(_._2.text.contains("Stored"))

    val procsStoredNames = procsStored map (_._1)

    val tableNames = procsLookupNames
      .filter(x => procsStoredNames.map(_.text.split('.')(0)).contains(x.text.split('.')(0)))
      .map(_.text.split('.').lift(1).toString.split('_').drop(1).reduceOption((x, y) => x + '_' + y).getOrElse("null"))
      .filter(_ != "null")
      .distinct
      .map(_.replaceFirst("_", "."))

    val file = new File("listOfTablesToCopyFromProd")

    val bw = new BufferedWriter(new FileWriter(file, true))

    tableNames.filter(
      !listOfTables.contains(_)
    ).map(
        st => bw.write(st + "\n")
      )

    bw.close()
  }

}
