# -*- coding: UTF-8 -*-
# MySQL Workbench Python script
# <description>
# Written in MySQL Workbench 8.0.26

import os
import sys
import grt
import workbench

def fixTable(table):
    table.temp_sql=""
    table.tableEngine=""
    table.defaultCharacterSetName=""

def fixColumn(table):
    notDefaultTypes=["TEXT"]
    intTypes=["TINYINT","SMAILLINT","MEDIUMINT","INT","BIGINT","FLOAT","DOUBLE","DECIMAL"]
    for column in table.columns:
        column.characterSetName=""
        for type in notDefaultTypes:
            if column.formattedType.startswith(type) and (column.defaultValue != ""):
                column.defaultValue = ""
                print("set default value to nil: {}".format(column.name))

        for type in intTypes:
            if column.formattedType.startswith(type) and (column.defaultValue == "''"):
                column.defaultValue = "'0'"
                print("set default value to 0: {}".format(column.name))

def fixForeignKey(table):
    for foreignKey in table.foreignKeys:
        foreignKey.modelOnly = 0 
        foreignKey.name = "fk_{}_{}".format(table.name,foreignKey.referencedTable.name)
        foreignKey.index.name="fk_{}_{}_idx".format(table.name,foreignKey.referencedTable.name)

def addColumns(catalog, table,name,typeStr,comment,isNotNull=1,defaultValue=""):
    datatypes = catalog.simpleDatatypes   
 
    # create a new column object and set its name
    column = grt.classes.db_mysql_Column()
    column.name = name
    column.comment = comment.encode('gbk').decode('utf-8')
    column.isNotNull = isNotNull
    column.defaultValue = defaultValue
    column.setParseType(typeStr,datatypes)
    table.columns.append(column)

def generateSQL(catalog, tables, file):
    fe = grt.modules.DbMySQLFE
    options = {'OmitSchemas':1,'KeepSchemata':1,'SkipForeignKeys':1, 'SkipFKIndexes':0,'GenerateDrops':0,'GenerateSchemaDrops':0}

    fe.generateSQLCreateStatements(catalog, catalog.version, options)
    def skip(obj):
        obj.temp_sql = "-- generated skip, no script was generated for %s" % obj.name

    schemata = catalog.schemata[0]
    skip(schemata)
    for table in schemata.tables:
        if tables is not None and table.name not in tables:
            skip(table)

    catalog.customData["migration:preamble"] = ''
    catalog.customData["migration:postamble"] = ''

    fe.createScriptForCatalogObjects(file, catalog, {})


def _main():
    if grt.root.wb.doc is None:
        return

    catalog = grt.root.wb.doc.physicalModels[0].catalog

    catalog.schemata[0].name="db"
    for table in catalog.schemata[0].tables:
        fixTable(table)
        fixColumn(table)
        fixForeignKey(table)

    generateSQL(catalog, modelTables, modelOut)


modelIn = os.getenv('IN')
modelOut = os.getenv('OUT')
if modelOut is None:
    modelOut = "./a.sql"

modelTables=os.getenv('TABLE_FILTER_LIST')
if modelTables is not None:
    modelTables = modelTables.split(",")

if modelIn is not None:
    grt.modules.Workbench.closeModelFile()
    grt.modules.Workbench.openModel(modelIn)

_main()
sys.exit()
