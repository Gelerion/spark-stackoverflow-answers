package org.apache.spark.sql

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{AtomicType, DataType, SQLUserDefinedType, StringType, UserDefinedType}
import org.apache.spark.unsafe.types.UTF8String

@SQLUserDefinedType(udt = classOf[FastStringUDT])
case class FastString(s: UTF8String) extends Comparable[FastString] with Externalizable with KryoSerializable {
  override def compareTo(o: FastString): Int = s.compareTo(o.s)

  override def writeExternal(out: ObjectOutput): Unit = s.writeExternal(out)
  override def readExternal(in: ObjectInput): Unit = s.readExternal(in)

  override def write(kryo: Kryo, output: Output): Unit = s.write(kryo, output)
  override def read(kryo: Kryo, input: Input): Unit = s.read(kryo, input)
}

class FastStringUDT extends UserDefinedType[FastString] {

  override def sqlType: DataType = StringType

  //override def pyUDT: String = "pyspark.sql.radekm.FastStringUDT"

  override def serialize(p: FastString): UTF8String = p.s
  override def deserialize(datum: Any): FastString = {
    datum match {
      case value: UTF8String => FastString(value.clone())
    }
  }

  override def userClass: Class[FastString] = classOf[FastString]
  private[spark] override def asNullable: FastStringUDT = this

  override private[sql] def unapply(e: Expression): Boolean = {
    println("unapply sql")
    e.dataType == this
  }

  def unapply(that: DataType): Option[StringType] = {
    println("unapply")
    Some(StringType)
  }

//  def unapply(e: Expression): Boolean = {
//    e.dataType.isInstanceOf[AtomicType]
//  }
}

case object FastStringUDT extends FastStringUDT {

  override private[sql] def unapply(e: Expression): Boolean = {
    println("unapply sql obj")
    e.dataType == this
  }
}