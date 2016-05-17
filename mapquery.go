package mapquery

import (
	"fmt"
	"database/sql"
)


/**
Used to map rows with unknown columns from a DB query so we can add them to a JSON response
*/
type MapStringScan struct {
	// cp are the column pointers
	cp []interface{}
	// row contains the final result
	row      map[string]string
	colCount int
	colNames []string
}

/**
Initialise a mop for a row in the DB query result that will be updated with `rows.Scan()`
*/
func newMapStringScan(columnNames []string) *MapStringScan {
	lenCN := len(columnNames)
	s := &MapStringScan{
		cp:       make([]interface{}, lenCN),
		row:      make(map[string]string, lenCN),
		colCount: lenCN,
		colNames: columnNames,
	}
	for i := 0; i < lenCN; i++ {
		s.cp[i] = new(sql.RawBytes)
	}
	return s
}

/**
Update a row map from the db query result
*/
func (s *MapStringScan) Update(rows *sql.Rows) error {
	if err := rows.Scan(s.cp...); err != nil {
		return err
	}

	for i := 0; i < s.colCount; i++ {
		if rb, ok := s.cp[i].(*sql.RawBytes); ok {
			s.row[s.colNames[i]] = string(*rb)

			*rb = nil // reset pointer to discard current value to avoid a bug
		} else {
			return fmt.Errorf("Cannot convert index %d column %s to type *sql.RawBytes", i, s.colNames[i])
		}
	}
	return nil
}

/**
Get a map representing a row from DB query results
*/
func (s *MapStringScan) Get() map[string]string {
	rowCopy := make(map[string]string, len(s.row))
	// Create a copy of the map for this row, as it will be updated for every row
	for k, v := range s.row {
		rowCopy[k] = v
	}

	return rowCopy
}

/**
Take the sql.Rows from a db query and return a slice of `map[string]string`s for the columns in each row
 */
func MapRows(rows *sql.Rows) ([]map[string]string, error) {

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var response []map[string]string

	rc := newMapStringScan(columnNames)
	for rows.Next() {
		err := rc.Update(rows)
		if err != nil {
			fmt.Println(err)
		}

		response = append(response, rc.Get())
	}
	rows.Close()

	return response, nil;
}