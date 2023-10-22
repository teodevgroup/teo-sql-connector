use crate::schema::dialect::SQLDialect;
use crate::schema::value::encode::ToSQLString;

pub(crate) struct SQLShowIndexFromStatement {
    pub(crate) table: String
}

impl ToSQLString for SQLShowIndexFromStatement {
    fn to_string(&self, _dialect: SQLDialect) -> String {
        let table = &self.table;
        format!("SHOW INDEX FROM {table}")
    }
}
