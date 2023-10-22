use crate::schema::dialect::SQLDialect;
use crate::schema::value::encode::ToSQLString;

pub(crate) struct SQLUseDatabaseStatement {
    pub(crate) database: String
}

impl ToSQLString for SQLUseDatabaseStatement {
    fn to_string(&self, _dialect: SQLDialect) -> String {
        let database = &self.database;
        format!("USE `{database}`")
    }
}
