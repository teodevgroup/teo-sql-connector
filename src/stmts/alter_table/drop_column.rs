use crate::schema::dialect::SQLDialect;
use crate::schema::value::encode::ToSQLString;

pub struct SQLAlterTableDropColumnStatement {
    pub(crate) table: String,
    pub(crate) column: String,
}

impl ToSQLString for SQLAlterTableDropColumnStatement {
    fn to_string(&self, dialect: SQLDialect) -> String {
        let table = &self.table;
        let column = &self.column;
        let escape = dialect.escape();
        format!("ALTER TABLE {escape}{table}{escape} DROP COLUMN {escape}{column}{escape}")
    }
}
