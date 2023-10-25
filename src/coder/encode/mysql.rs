use teo_parser::r#type::Type;
use teo_runtime::database::mysql::r#type::MySQLType;
use teo_teon::Value;
use crate::schema::value::encode::ToSQLInput;

pub fn encode_value(value: &Value, t: &Type, dt: &MySQLType) -> String {
    if value.is_null() {
        "NULL".to_string()
    }
    let t = t.unwrap_optional();
    if t.is_bool() {
        return value.as_bool().unwrap().to_sql_input();
    }
}