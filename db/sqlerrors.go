package db

import "fmt"

func InvalidTblErr(name string) error {
	return fmt.Errorf("table %s does not exist in the schema cache. You may need to call /schema/invalidate if the schema cache is stale", name)
}

func InvalidColErr(colName, tblName string) error {
	return fmt.Errorf("column %s does not exist on table %s in the schema cache. You may need to call /schema/invalidate if the schema cache is stale", colName, tblName)
}

func InvalidTypeErr(column, typeName string) error {
	return fmt.Errorf("type %s is not a valid type for column %s", typeName, column)
}
