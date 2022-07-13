package dct.json

import org.scalatest.funsuite.AnyFunSuite
import java.net.{URLDecoder => decoder}

class JsonADFMappingConverterTestSuit extends AnyFunSuite {

  val testResourceRoot: String = decoder.
    decode(getClass.getResource("/").getPath, "UTF-8")

  @transient val testMapFiles: Array[String] = new java.io.File(testResourceRoot).
    listFiles.
    filter(f => f.isFile && f.getPath.endsWith(".json")).
    map(_.getPath)

  test("deserialize all adf map files") {
    assertResult(41)(testMapFiles.map(ADFMapping(_).mappingSchemaAsStructType).length)
  }

  test("print map aliases") {
    val expectedRes = Map(
        "Outlets_Cluster"->"Outlets_Cluster",
        "MarsRegion_ENGName"->"MarsRegion_ENGName",
        "CITY"->"CITY",
        "Outlets_Filial"->"Outlets_Filial",
        "Outlets_AddressCity"->"Outlets_AddressCity",
        "Chain_GHierarchyCode"->"Chain_GHierarchyCode",
        "__POS_HASH_ROW__"->"POS_ID",
        "BRANCH"->"BRANCH",
        "Outlets_AddressRegion"->"Outlets_AddressRegion",
        "FORMAT"->"FORMAT",
        "Outlets_GLN"->"Outlets_GLN",
        "DC"->"DC",
        "POS_CODE"->"POS_CODE",
        "POS_NAME"->"POS_NAME",
        "Outlets_Name"->"Outlets_Name",
        "Outlets_Subname"->"Outlets_Subname",
        "ADDRESS"->"ADDRESS",
        "Chain_DistributorENGName"->"Chain_DistributorENGName",
        "__INGEST_TS__"->"INGEST_TIMESTAMP",
        "Outlets_Format2"->"Outlets_Format2",
        "Outlets_Code"->"Outlets_Code",
        "Chain_Code"->"Chain_Code",
        "Outlets_OutletType"->"Outlets_OutletType",
        "Distributor_Code"->"Distributor_Code",
        "Chain_SystemName"->"Chain_SystemName",
    )

    val testMapPath = testMapFiles.filter(_.contains("DICTIONARY_POS")).head
    assertResult(expectedRes)(ADFMapping(testMapPath).aliasMap)
  }

}
