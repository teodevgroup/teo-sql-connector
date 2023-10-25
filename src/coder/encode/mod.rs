pub mod mysql;
pub mod postgres;
pub mod sqlite;

use teo_parser::r#type::Type;
use teo_runtime::database::r#type::DatabaseType;
use teo_teon::Value;

pub fn encode_value(v: &Value, t: &Type, dt: &DatabaseType) -> String {
    match dt {
        DatabaseType::MySQLType(dt) => mysql::encode_value(v, t, dt),
        DatabaseType::PostgreSQLType(dt) => postgres::encode_value(v, t, dt),
        DatabaseType::SQLiteType(dt) => sqlite::encode_value(v, t, dt),
        _ => panic!(),
    }
}