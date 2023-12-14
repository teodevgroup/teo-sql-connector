use teo_runtime::database::mysql::r#type::MySQLType;
use teo_runtime::database::postgres::r#type::PostgreSQLType;
use teo_runtime::database::r#type::DatabaseType;
use teo_runtime::database::sqlite::r#type::SQLiteType;

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum SQLDialect {
    MySQL,
    PostgreSQL,
    SQLite,
    MSSQL,
}

impl SQLDialect {

    pub(crate) fn escape(&self) -> &'static str {
        match self {
            SQLDialect::PostgreSQL => "\"",
            _ => "`",
        }
    }

    pub(crate) fn is_postgres(&self) -> bool {
        match self {
            SQLDialect::PostgreSQL => true,
            _ => false,
        }
    }

    pub(crate) fn is_mysql(&self) -> bool {
        match self {
            SQLDialect::MySQL => true,
            _ => false,
        }
    }

    pub(crate) fn is_sqlite(&self) -> bool {
        match self {
            SQLDialect::SQLite => true,
            _ => false,
        }
    }

    pub(crate) fn float64_type(&self) -> DatabaseType {
        match self {
            SQLDialect::MySQL => DatabaseType::MySQLType(MySQLType::Double),
            SQLDialect::PostgreSQL => DatabaseType::PostgreSQLType(PostgreSQLType::DoublePrecision),
            SQLDialect::SQLite => DatabaseType::SQLiteType(SQLiteType::Real),
            SQLDialect::MSSQL => panic!(),
        }
    }

    pub(crate) fn int64_type(&self) -> DatabaseType {
        match self {
            SQLDialect::MySQL => DatabaseType::MySQLType(MySQLType::Int(None, true)),
            SQLDialect::PostgreSQL => DatabaseType::PostgreSQLType(PostgreSQLType::Integer),
            SQLDialect::SQLite => DatabaseType::SQLiteType(SQLiteType::Integer),
            SQLDialect::MSSQL => panic!(),
        }
    }
}
